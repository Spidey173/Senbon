WITH source AS (
    SELECT * FROM {{ source('retail_source', 'stg_suppliers_raw') }}
),

renamed AS (
    SELECT
        TRIM(supplier_id) AS supplier_id,
        TRIM(supplier_name) AS supplier_name,
        LOWER(TRIM(contact_email)) AS contact_email,
        UPPER(TRIM(country)) AS country,
        UPPER(TRIM(payment_terms)) AS payment_terms,
        {{ audit_columns() }}
    FROM source
    WHERE supplier_id IS NOT NULL
)

SELECT * FROM renamed

