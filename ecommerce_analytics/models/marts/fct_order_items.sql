{{ config(materialized='table') }}

SELECT
    order_id,
    order_item_id,
    product_id,
    unit_price,
    order_quantity,
    product_name,
    product_category
FROM {{ ref('int_order_items_enriched') }}
