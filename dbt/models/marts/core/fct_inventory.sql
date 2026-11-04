{{ config(materialized='table') }}

WITH inventory AS (
    SELECT * FROM {{ ref('stg_retail__inventory') }}
),

products AS (
    SELECT product_key, product_id, cost_price FROM {{ ref('dim_products') }} WHERE is_current = TRUE
),

stores AS (
    SELECT store_key, store_id FROM {{ ref('dim_stores') }}
)

SELECT
    {{ generate_surrogate_key(['i.inventory_id']) }} AS inventory_key,
    CAST(TO_CHAR(CURRENT_DATE, 'YYYYMMDD') AS INT) AS date_key,
    COALESCE(s.store_key, 'UNKNOWN_STORE') AS store_key,
    COALESCE(p.product_key, 'UNKNOWN_PRODUCT') AS product_key,
    i.quantity_on_hand,
    i.reorder_level,
    i.stockout_risk,
    ROUND(i.quantity_on_hand * COALESCE(p.cost_price, 25.0), 2) AS inventory_value_usd,
    {{ audit_columns() }}
FROM inventory i
LEFT JOIN stores s ON i.store_id = s.store_id
LEFT JOIN products p ON i.product_id = p.product_id

