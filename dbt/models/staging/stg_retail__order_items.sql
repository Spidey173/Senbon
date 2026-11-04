WITH source AS (
    SELECT * FROM {{ source('retail_source', 'stg_order_items_raw') }}
),

renamed AS (
    SELECT
        TRIM(order_item_id) AS order_item_id,
        TRIM(order_id) AS order_id,
        TRIM(product_id) AS product_id,
        CAST(quantity AS INT) AS quantity,
        CAST(unit_price AS NUMERIC(12, 2)) AS unit_price,
        CAST(total_line_amount AS NUMERIC(14, 2)) AS total_line_amount,
        {{ audit_columns() }}
    FROM source
    WHERE order_item_id IS NOT NULL AND quantity > 0
)

SELECT * FROM renamed

