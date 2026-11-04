WITH source AS (
    SELECT * FROM {{ source('retail_source', 'stg_customers_raw') }}
),

renamed AS (
    SELECT
        TRIM(customer_id) AS customer_id,
        INITCAP(TRIM(first_name)) AS first_name,
        INITCAP(TRIM(last_name)) AS last_name,
        CONCAT(INITCAP(TRIM(first_name)), ' ', INITCAP(TRIM(last_name))) AS full_name,
        LOWER(TRIM(email)) AS email,
        TRIM(phone) AS phone,
        TRIM(city) AS city,
        UPPER(TRIM(state)) AS state,
        UPPER(TRIM(country)) AS country,
        TRIM(postal_code) AS postal_code,
        UPPER(TRIM(customer_segment)) AS customer_segment,
        CAST(created_at AS TIMESTAMP) AS created_at,
        CAST(updated_at AS TIMESTAMP) AS updated_at,
        {{ audit_columns() }}
    FROM source
    WHERE customer_id IS NOT NULL AND customer_id != ''
)

SELECT * FROM renamed

