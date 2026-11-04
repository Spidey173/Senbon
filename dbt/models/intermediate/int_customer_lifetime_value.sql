WITH customers AS (
    SELECT * FROM {{ ref('stg_retail__customers') }}
),

orders AS (
    SELECT
        customer_id,
        COUNT(DISTINCT order_id) AS lifetime_orders_count,
        SUM(net_amount) AS lifetime_spend_usd,
        AVG(net_amount) AS average_order_value_usd,
        MIN(order_date) AS first_order_date,
        MAX(order_date) AS last_order_date
    FROM {{ ref('stg_retail__orders') }}
    WHERE order_status = 'COMPLETED'
    GROUP BY 1
)

SELECT
    c.customer_id,
    c.full_name,
    c.email,
    c.customer_segment,
    c.city,
    c.state,
    COALESCE(o.lifetime_orders_count, 0) AS lifetime_orders_count,
    COALESCE(ROUND(o.lifetime_spend_usd, 2), 0.00) AS lifetime_spend_usd,
    COALESCE(ROUND(o.average_order_value_usd, 2), 0.00) AS average_order_value_usd,
    o.first_order_date,
    o.last_order_date,
    CASE
        WHEN o.lifetime_spend_usd >= 1000 THEN 'PLATINUM'
        WHEN o.lifetime_spend_usd >= 500 THEN 'GOLD'
        WHEN o.lifetime_spend_usd > 0 THEN 'SILVER'
        ELSE 'PROSPECT'
    END AS lifetime_value_tier,
    {{ audit_columns() }}
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id

