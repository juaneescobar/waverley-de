SELECT 
    p.product_id,
    p.product_name,
    SUM(oi.quantity * oi.price) AS total_revenue,
    SUM(oi.quantity) AS total_quantity_sold
FROM  {{ ref('fact_orders') }} oi
JOIN {{ ref('dim_products') }} p ON oi.product_id = p.product_id
GROUP BY p.product_id, p.product_name
ORDER BY total_revenue DESC
LIMIT 3