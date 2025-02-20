
{{ config(
    materialized='incremental',
    unique_key='order_item_id',
    on_schema_change='fail'
) }}

SELECT * FROM  {{ ref('stg_order_items') }}
