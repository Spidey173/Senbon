WITH source AS (
    SELECT * FROM {{ source('retail_source', 'stg_payments_raw') }}
),

renamed AS (
    SELECT
        TRIM(payment_id) AS payment_id,
        TRIM(order_id) AS order_id,
        UPPER(TRIM(payment_method)) AS payment_method,
        UPPER(TRIM(payment_status)) AS payment_status,
        CAST(amount AS NUMERIC(14, 2)) AS amount,
        UPPER(TRIM(currency)) AS currency,
        CAST(payment_date AS TIMESTAMP) AS payment_date,
        {{ audit_columns() }}
    FROM source
    WHERE payment_id IS NOT NULL AND amount >= 0
)

SELECT * FROM renamed

