-- Test to guarantee no negative order amounts enter production facts
SELECT
    order_id,
    net_amount
FROM {{ ref('fct_orders') }}
WHERE net_amount < 0

