{{ config(
    materialized='incremental',
    unique_key='order_id',
    on_schema_change='fail',
    post_hook="
        DROP TABLE IF EXISTS stg.order_items;
        DROP TABLE IF EXISTS stg.orders;
    "
) }}

WITH order_items AS (
    SELECT 
        order_id,
        product_id,
        quantity,
        price
    FROM {{ ref('stg_order_items') }}
),
orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
)

SELECT
    o.order_id,
    o.customer_id,
    o.order_date,
    oi.product_id,
    oi.quantity,
    oi.price
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id

{% if is_incremental() %}
WHERE o.order_date > (SELECT MAX(order_date) FROM {{ this }})  -- Load only new data
{% endif %}
