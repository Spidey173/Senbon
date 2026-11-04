-- Test to guarantee no impossible future orders exist in fact table
SELECT
    order_id,
    order_date
FROM {{ ref('stg_retail__orders') }}
WHERE order_date > CURRENT_TIMESTAMP + INTERVAL '1 day'

