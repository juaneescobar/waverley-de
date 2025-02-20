{{ config(materialized='table') }}

WITH fact_orders AS (
    SELECT * FROM {{ ref('fact_orders') }}
),
products AS (
    SELECT * FROM {{ ref('dim_products') }}
)

SELECT
    fo.order_id,
    fo.customer_id,
    fo.order_date,
    SUM(fo.quantity * fo.price) AS revenue_per_order,
    COUNT(DISTINCT fo.product_id) AS products_per_order
FROM fact_orders fo
JOIN products p ON fo.product_id = p.product_id
GROUP BY fo.order_id, fo.customer_id, fo.order_date