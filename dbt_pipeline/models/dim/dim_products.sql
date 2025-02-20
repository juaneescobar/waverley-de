{{ config(
    post_hook="
        DROP TABLE IF EXISTS stg.products;
    "
) }}

WITH dim_products AS (
    SELECT
        -- Ensure product_id starts with 'P' followed by digits (e.g., P001)
        CASE
            WHEN product_id ~ '^P[0-9]+$' THEN product_id
            ELSE NULL  -- Set to NULL if it doesn't start with 'P' followed by digits
        END AS product_id,
        
        -- Set product_name to NULL if it matches any category or if it's not a valid string
        CASE
            WHEN LOWER(TRIM(product_name)) IN ('footwear', 'electronics', 'kitchen', 'bags', 'accessories', 'apparel') 
                 OR product_name ~ '^\d+$'  -- Checks if product_name is numeric
                 OR product_name IS NULL  -- Also set to NULL if the value is already NULL
            THEN NULL
            ELSE INITCAP(TRIM(product_name))  -- Otherwise, capitalize and clean the product name
        END AS product_name,
        
        -- Clean category: only allow specific values, set to NULL if not valid
        CASE
            WHEN LOWER(TRIM(category)) IN ('footwear', 'electronics', 'kitchen', 'bags', 'accessories', 'apparel') THEN INITCAP(TRIM(category))
            ELSE NULL
        END AS category,
        
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
        END AS price,
    CURRENT_TIMESTAMP AS updated_at
    FROM stg.products
)

SELECT * FROM dim_products WHERE product_id IS NOT NULL