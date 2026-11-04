WITH inventory AS (
    SELECT * FROM {{ ref('stg_retail__inventory') }}
),

products AS (
    SELECT
        product_id,
        product_name,
        category,
        unit_price,
        cost_price
    FROM {{ ref('stg_retail__products') }}
)

SELECT
    i.inventory_id,
    i.store_id,
    i.product_id,
    p.product_name,
    p.category,
    i.quantity_on_hand,
    i.reorder_level,
    i.stockout_risk,
    ROUND(i.quantity_on_hand * p.cost_price, 2) AS stock_valuation_at_cost_usd,
    ROUND(i.quantity_on_hand * p.unit_price, 2) AS stock_potential_revenue_usd,
    i.last_restocked_at,
    {{ audit_columns() }}
FROM inventory i
LEFT JOIN products p ON i.product_id = p.product_id

