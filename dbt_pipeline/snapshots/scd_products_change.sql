{% snapshot scd_products_change %}

{{ config(
    target_schema='snapshots',
    unique_key='product_id',
    strategy='timestamp',
    updated_at='updated_at'
) }}

SELECT 
    product_id, 
    product_name, 
    category, 
    price, 
    updated_at
FROM {{ ref('dim_products') }}

{% endsnapshot %}