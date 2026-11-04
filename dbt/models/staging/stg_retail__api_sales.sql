WITH source AS (
    SELECT * FROM {{ source('retail_source', 'stg_api_sales_raw') }}
),

renamed AS (
    SELECT
        TRIM(api_sale_id) AS api_sale_id,
        TRIM(store_id) AS store_id,
        UPPER(TRIM(channel)) AS sales_channel,
        CAST(sale_timestamp AS TIMESTAMP) AS sale_timestamp,
        CAST(item_count AS INT) AS item_count,
        CAST(gross_amount AS NUMERIC(14, 2)) AS gross_amount,
        CAST(discount_amount AS NUMERIC(12, 2)) AS discount_amount,
        CAST(net_amount AS NUMERIC(14, 2)) AS net_amount,
        UPPER(TRIM(currency)) AS currency,
        UPPER(TRIM(payment_type)) AS payment_type,
        {{ audit_columns() }}
    FROM source
    WHERE api_sale_id IS NOT NULL
)

SELECT * FROM renamed

