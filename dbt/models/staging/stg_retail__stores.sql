WITH source AS (
    SELECT * FROM {{ source('retail_source', 'stg_stores_raw') }}
),

renamed AS (
    SELECT
        TRIM(store_id) AS store_id,
        TRIM(store_name) AS store_name,
        UPPER(TRIM(store_type)) AS store_type,
        TRIM(city) AS city,
        UPPER(TRIM(state)) AS state,
        UPPER(TRIM(country)) AS country,
        CAST(square_feet AS INT) AS square_feet,
        CAST(opened_date AS DATE) AS opened_date,
        {{ audit_columns() }}
    FROM source
    WHERE store_id IS NOT NULL
)

SELECT * FROM renamed

