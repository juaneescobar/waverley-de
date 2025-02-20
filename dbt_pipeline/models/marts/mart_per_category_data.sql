{{ config(materialized='table') }}

WITH fact_orders AS (
    SELECT * FROM {{ ref('fact_orders') }}
),
products AS (
    SELECT * FROM {{ ref('dim_products') }}
)

SELECT
    p.category,
    COUNT(DISTINCT fo.customer_id) AS distinct_customers_per_category
FROM fact_orders fo
JOIN products p ON fo.product_id = p.product_id
GROUP BY fo.order_id, fo.customer_id, fo.order_date, p.category