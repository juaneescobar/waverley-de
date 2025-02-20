{{ config(
    post_hook="DROP TABLE IF EXISTS stg.orders"
) }}

WITH raw_orders AS (
    SELECT
        order_id,
        customer_id,
        order_date::DATE AS order_date,
        total_amount::NUMERIC AS total_amount
    FROM {{ source('retail', 'orders') }}
    WHERE order_id IS NOT NULL
)

SELECT * FROM raw_orders