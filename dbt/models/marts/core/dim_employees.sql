{{ config(materialized='table') }}

WITH employees AS (
    SELECT * FROM {{ ref('stg_retail__employees') }}
)

SELECT
    {{ generate_surrogate_key(['employee_id']) }} AS employee_key,
    employee_id,
    store_id,
    full_name,
    role,
    hourly_rate,
    annual_salary,
    hire_date,
    {{ audit_columns() }}
FROM employees

