{{ config(materialized='table') }}

WITH stores AS (
    SELECT * FROM {{ ref('stg_retail__stores') }}
)

SELECT
    {{ generate_surrogate_key(['store_id']) }} AS store_key,
    store_id,
    store_name,
    store_type,
    city,
    state,
    country,
    square_feet,
    opened_date,
    {{ audit_columns() }}
FROM stores

