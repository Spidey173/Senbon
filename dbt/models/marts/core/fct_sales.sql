{{ config(
    materialized='incremental',
    unique_key='sale_key'
) }}

WITH order_items AS (
    SELECT * FROM {{ ref('stg_retail__order_items') }}
    {% if is_incremental() %}
    WHERE _lakehouse_ingested_at > (SELECT COALESCE(MAX(_lakehouse_ingested_at), '1970-01-01') FROM {{ this }})
    {% endif %}
),

orders AS (
    SELECT order_id, order_date, order_date_day, customer_id, store_id FROM {{ ref('stg_retail__orders') }}
),

products AS (
    SELECT product_key, product_id, cost_price, valid_from, valid_to FROM {{ ref('dim_products') }}
),

dim_dates AS (
    SELECT date_key, calendar_date FROM {{ ref('dim_dates') }}
),

dim_customers AS (
    SELECT customer_key, customer_id, valid_from, valid_to FROM {{ ref('dim_customers') }}
),

dim_stores AS (
    SELECT store_key, store_id FROM {{ ref('dim_stores') }}
)

SELECT
    {{ generate_surrogate_key(['i.order_item_id']) }} AS sale_key,
    {{ generate_surrogate_key(['i.order_id']) }} AS order_key,
    d.date_key,
    COALESCE(c.customer_key, 'UNKNOWN_CUSTOMER') AS customer_key,
    COALESCE(s.store_key, 'UNKNOWN_STORE') AS store_key,
    COALESCE(p.product_key, 'UNKNOWN_PRODUCT') AS product_key,
    i.quantity,
    i.unit_price,
    i.total_line_amount AS line_total,
    ROUND(i.quantity * COALESCE(p.cost_price, i.unit_price * 0.6), 2) AS line_cost,
    ROUND(i.total_line_amount - (i.quantity * COALESCE(p.cost_price, i.unit_price * 0.6)), 2) AS line_profit,
    ROUND(
        ((i.total_line_amount - (i.quantity * COALESCE(p.cost_price, i.unit_price * 0.6))) / NULLIF(i.total_line_amount, 0)) * 100,
        2
    ) AS profit_margin_pct,
    {{ audit_columns() }}
FROM order_items i
INNER JOIN orders o ON i.order_id = o.order_id
LEFT JOIN dim_dates d ON o.order_date_day = d.calendar_date
LEFT JOIN dim_customers c 
    ON o.customer_id = c.customer_id
   AND o.order_date >= c.valid_from
   AND (o.order_date < c.valid_to OR c.valid_to IS NULL)
LEFT JOIN dim_stores s ON o.store_id = s.store_id
LEFT JOIN products p 
    ON i.product_id = p.product_id
   AND o.order_date >= p.valid_from
   AND (o.order_date < p.valid_to OR p.valid_to IS NULL)

