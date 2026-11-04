WITH source AS (
    SELECT * FROM {{ source('retail_source', 'stg_employees_raw') }}
),

renamed AS (
    SELECT
        TRIM(employee_id) AS employee_id,
        TRIM(store_id) AS store_id,
        INITCAP(TRIM(first_name)) AS first_name,
        INITCAP(TRIM(last_name)) AS last_name,
        CONCAT(INITCAP(TRIM(first_name)), ' ', INITCAP(TRIM(last_name))) AS full_name,
        TRIM(role) AS role,
        CAST(hourly_rate AS NUMERIC(10, 2)) AS hourly_rate,
        CAST(ROUND(hourly_rate * 2080, 2) AS NUMERIC(12, 2)) AS annual_salary,
        CAST(hire_date AS DATE) AS hire_date,
        {{ audit_columns() }}
    FROM source
    WHERE employee_id IS NOT NULL
)

SELECT * FROM renamed

