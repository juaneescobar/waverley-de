SELECT
    DATE_TRUNC('day', order_date) AS order_day,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(quantity * price) AS total_revenue
FROM {{ ref('fact_orders') }}
GROUP BY order_day
ORDER BY order_day