{{ config(materialized='table') }}

-- Generates calendar dimension records
WITH date_series AS (
    SELECT
        CAST('2025-01-01' AS DATE) + (i * INTERVAL '1 day') AS calendar_date
    FROM generate_series(0, 1095) AS i
)

SELECT
    CAST(TO_CHAR(calendar_date, 'YYYYMMDD') AS INT) AS date_key,
    calendar_date,
    EXTRACT(YEAR FROM calendar_date)::INT AS year,
    EXTRACT(QUARTER FROM calendar_date)::INT AS quarter,
    EXTRACT(MONTH FROM calendar_date)::INT AS month,
    TO_CHAR(calendar_date, 'Month') AS month_name,
    EXTRACT(WEEK FROM calendar_date)::INT AS week_of_year,
    EXTRACT(DAY FROM calendar_date)::INT AS day_of_month,
    EXTRACT(DOW FROM calendar_date)::INT + 1 AS day_of_week,
    TO_CHAR(calendar_date, 'Day') AS day_name,
    CASE WHEN EXTRACT(DOW FROM calendar_date) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
    CASE WHEN EXTRACT(MONTH FROM calendar_date) >= 2 THEN EXTRACT(YEAR FROM calendar_date)::INT ELSE (EXTRACT(YEAR FROM calendar_date) - 1)::INT END AS fiscal_year,
    (((EXTRACT(MONTH FROM calendar_date)::INT - 2 + 12) % 12) / 3 + 1)::INT AS fiscal_quarter
FROM date_series

