{% snapshot scd_products_change %}

{% if dbt_utils.is_relation(ref('dim_products')) %}

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

{% else %}
    {{ exceptions.raise_compiler_error("The table 'dim_products' does not exist. Please ensure it is created before running this snapshot.") }}
{% endif %}

{% endsnapshot %}
