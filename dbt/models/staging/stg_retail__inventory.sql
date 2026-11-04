WITH source AS (
    SELECT * FROM {{ source('retail_source', 'stg_inventory_raw') }}
),

renamed AS (
    SELECT
        TRIM(inventory_id) AS inventory_id,
        TRIM(store_id) AS store_id,
        TRIM(product_id) AS product_id,
        CAST(quantity_on_hand AS INT) AS quantity_on_hand,
        CAST(reorder_level AS INT) AS reorder_level,
        COALESCE(CAST(stockout_risk AS BOOLEAN), (quantity_on_hand <= reorder_level)) AS stockout_risk,
        CAST(last_restocked_at AS TIMESTAMP) AS last_restocked_at,
        {{ audit_columns() }}
    FROM source
    WHERE inventory_id IS NOT NULL
)

SELECT * FROM renamed

