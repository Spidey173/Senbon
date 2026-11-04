WITH source AS (
    SELECT * FROM {{ source('retail_source', 'stg_products_raw') }}
),

renamed AS (
    SELECT
        TRIM(product_id) AS product_id,
        TRIM(product_name) AS product_name,
        TRIM(category) AS category,
        TRIM(subcategory) AS subcategory,
        CAST(unit_price AS NUMERIC(12, 2)) AS unit_price,
        CAST(cost_price AS NUMERIC(12, 2)) AS cost_price,
        CAST(ROUND(unit_price - cost_price, 2) AS NUMERIC(12, 2)) AS gross_margin_usd,
        TRIM(sku) AS sku,
        COALESCE(CAST(is_active AS BOOLEAN), TRUE) AS is_active,
        CAST(created_at AS TIMESTAMP) AS created_at,
        CAST(updated_at AS TIMESTAMP) AS updated_at,
        {{ audit_columns() }}
    FROM source
    WHERE product_id IS NOT NULL AND product_id != ''
)

SELECT * FROM renamed

