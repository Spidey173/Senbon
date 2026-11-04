{{ config(materialized='table') }}

WITH scd_source AS (
    SELECT * FROM {{ ref('snap_products_scd2') }}
)

SELECT
    {{ generate_surrogate_key(['product_id', 'dbt_valid_from']) }} AS product_key,
    product_id,
    product_name,
    category,
    subcategory,
    unit_price,
    cost_price,
    gross_margin_usd,
    sku,
    is_active,
    dbt_valid_from AS valid_from,
    dbt_valid_to AS valid_to,
    CASE WHEN dbt_valid_to IS NULL THEN TRUE ELSE FALSE END AS is_current,
    {{ audit_columns() }}
FROM scd_source

