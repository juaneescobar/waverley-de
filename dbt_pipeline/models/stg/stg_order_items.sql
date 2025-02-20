{{ config(
    post_hook="DROP TABLE IF EXISTS stg.order_items"
) }}

WITH raw_order_items AS (
    SELECT
        order_item_id,
        order_id,
        product_id,
        quantity::INTEGER AS quantity,
        -- Clean price, split by space and take the first part, ensuring it's numeric
        CASE
            WHEN price IS NOT NULL THEN
                CAST(
                    CASE
                        WHEN SPLIT_PART(price, ' ', 1) ~ '^[0-9]+(\.[0-9]+)?$' THEN SPLIT_PART(price, ' ', 1)
                        ELSE NULL
                    END AS DECIMAL
                )
            ELSE NULL
        END AS price
    FROM {{ source('retail', 'order_items') }}
    WHERE order_item_id IS NOT NULL
)

SELECT  *, (quantity * price) AS revenue_per_order_item FROM raw_order_items