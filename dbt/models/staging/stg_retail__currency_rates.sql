WITH source AS (
    SELECT * FROM {{ source('retail_source', 'stg_currency_rates_raw') }}
),

renamed AS (
    SELECT
        UPPER(TRIM(base_currency)) AS base_currency,
        UPPER(TRIM(target_currency)) AS target_currency,
        CAST(exchange_rate AS NUMERIC(10, 4)) AS exchange_rate,
        CAST(effective_date AS DATE) AS effective_date,
        {{ audit_columns() }}
    FROM source
)

SELECT * FROM renamed

