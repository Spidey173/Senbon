WITH source AS (
    SELECT * FROM {{ source('retail_source', 'stg_orders_raw') }}
),

renamed AS (
    SELECT
        TRIM(order_id) AS order_id,
        TRIM(customer_id) AS customer_id,
        TRIM(store_id) AS store_id,
        TRIM(employee_id) AS employee_id,
        UPPER(TRIM(order_status)) AS order_status,
        CAST(order_date AS TIMESTAMP) AS order_date,
        CAST(order_date AS DATE) AS order_date_day,
        CAST(total_amount AS NUMERIC(14, 2)) AS total_amount,
        CAST(discount_amount AS NUMERIC(12, 2)) AS discount_amount,
        CAST(ROUND(total_amount, 2) AS NUMERIC(14, 2)) AS net_amount,
        CAST(updated_at AS TIMESTAMP) AS updated_at,
        COALESCE(CAST(is_late_arriving AS BOOLEAN), FALSE) AS is_late_arriving,
        {{ audit_columns() }}
    FROM source
    WHERE order_id IS NOT NULL AND total_amount >= 0
)

SELECT * FROM renamed

