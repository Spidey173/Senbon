WITH orders AS (
    SELECT * FROM {{ ref('stg_retail__orders') }}
),

items AS (
    SELECT
        order_id,
        COUNT(order_item_id) AS total_items_count,
        SUM(quantity) AS total_units_sold,
        SUM(total_line_amount) AS computed_gross_lines_amount
    FROM {{ ref('stg_retail__order_items') }}
    GROUP BY 1
),

payments AS (
    SELECT
        order_id,
        payment_method,
        payment_status,
        amount AS payment_amount,
        currency
    FROM {{ ref('stg_retail__payments') }}
)

SELECT
    o.order_id,
    o.customer_id,
    o.store_id,
    o.employee_id,
    o.order_status,
    o.order_date,
    o.order_date_day,
    o.total_amount AS order_header_amount,
    o.discount_amount,
    o.net_amount,
    COALESCE(i.total_items_count, 0) AS total_items_count,
    COALESCE(i.total_units_sold, 0) AS total_units_sold,
    COALESCE(i.computed_gross_lines_amount, o.total_amount) AS computed_gross_lines_amount,
    p.payment_method,
    COALESCE(p.payment_status, 'UNKNOWN') AS payment_status,
    o.is_late_arriving,
    {{ audit_columns() }}
FROM orders o
LEFT JOIN items i ON o.order_id = i.order_id
LEFT JOIN payments p ON o.order_id = p.order_id

