{{ config(materialized='table') }}

WITH scd_source AS (
    SELECT * FROM {{ ref('snap_customers_scd2') }}
)

SELECT
    {{ generate_surrogate_key(['customer_id', 'dbt_valid_from']) }} AS customer_key,
    customer_id,
    first_name,
    last_name,
    CONCAT(first_name, ' ', last_name) AS full_name,
    email,
    phone,
    city,
    state,
    country,
    postal_code,
    customer_segment,
    dbt_valid_from AS valid_from,
    dbt_valid_to AS valid_to,
    CASE WHEN dbt_valid_to IS NULL THEN TRUE ELSE FALSE END AS is_current,
    {{ audit_columns() }}
FROM scd_source

