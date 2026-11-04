{{ config(materialized='table') }}

WITH suppliers AS (
    SELECT * FROM {{ ref('stg_retail__suppliers') }}
)

SELECT
    {{ generate_surrogate_key(['supplier_id']) }} AS supplier_key,
    supplier_id,
    supplier_name,
    contact_email,
    country,
    payment_terms,
    {{ audit_columns() }}
FROM suppliers

