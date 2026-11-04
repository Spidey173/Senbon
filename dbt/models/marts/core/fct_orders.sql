{{ config(
    materialized='incremental',
    unique_key='order_key'
) }}

WITH orders AS (
    SELECT * FROM {{ ref('stg_retail__orders') }}
    {% if is_incremental() %}
    WHERE order_date > (SELECT COALESCE(MAX(order_date), '1970-01-01') FROM {{ this }})
    {% endif %}
),

dim_dates AS (
    SELECT date_key, calendar_date FROM {{ ref('dim_dates') }}
),

dim_customers AS (
    SELECT customer_key, customer_id, valid_from, valid_to FROM {{ ref('dim_customers') }}
),

dim_stores AS (
    SELECT store_key, store_id FROM {{ ref('dim_stores') }}
),

dim_employees AS (
    SELECT employee_key, employee_id FROM {{ ref('dim_employees') }}
)

SELECT
    {{ generate_surrogate_key(['o.order_id']) }} AS order_key,
    o.order_id,
    d.date_key,
    COALESCE(c.customer_key, 'UNKNOWN_CUSTOMER') AS customer_key,
    COALESCE(s.store_key, 'UNKNOWN_STORE') AS store_key,
    COALESCE(e.employee_key, 'UNKNOWN_EMPLOYEE') AS employee_key,
    o.order_status,
    o.total_amount,
    o.discount_amount,
    o.net_amount,
    o.is_late_arriving,
    {{ audit_columns() }}
FROM orders o
LEFT JOIN dim_dates d ON o.order_date_day = d.calendar_date
LEFT JOIN dim_customers c 
    ON o.customer_id = c.customer_id
   AND o.order_date >= c.valid_from
   AND (o.order_date < c.valid_to OR c.valid_to IS NULL)
LEFT JOIN dim_stores s ON o.store_id = s.store_id
LEFT JOIN dim_employees e ON o.employee_id = e.employee_id

