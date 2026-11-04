WITH stores AS (
    SELECT * FROM {{ ref('stg_retail__stores') }}
),

orders AS (
    SELECT
        store_id,
        COUNT(DISTINCT order_id) AS total_store_orders,
        SUM(net_amount) AS total_revenue_usd,
        AVG(net_amount) AS average_basket_usd
    FROM {{ ref('stg_retail__orders') }}
    WHERE order_status = 'COMPLETED'
    GROUP BY 1
),

employees AS (
    SELECT
        store_id,
        COUNT(DISTINCT employee_id) AS total_store_headcount,
        SUM(hourly_rate) AS total_hourly_payroll
    FROM {{ ref('stg_retail__employees') }}
    GROUP BY 1
)

SELECT
    s.store_id,
    s.store_name,
    s.store_type,
    s.city,
    s.state,
    s.square_feet,
    COALESCE(o.total_store_orders, 0) AS total_store_orders,
    COALESCE(ROUND(o.total_revenue_usd, 2), 0.00) AS total_revenue_usd,
    COALESCE(ROUND(o.average_basket_usd, 2), 0.00) AS average_basket_usd,
    COALESCE(e.total_store_headcount, 0) AS total_store_headcount,
    ROUND(COALESCE(o.total_revenue_usd, 0.00) / NULLIF(s.square_feet, 0), 2) AS revenue_per_sq_ft,
    {{ audit_columns() }}
FROM stores s
LEFT JOIN orders o ON s.store_id = o.store_id
LEFT JOIN employees e ON s.store_id = e.store_id

