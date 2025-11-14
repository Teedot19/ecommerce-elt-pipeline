{{ config(materialized='table') }}

SELECT
    product_id,
    product_name,
    product_category,
    price,
    stock_count,
    created_at as created_date,
    is_active,
    ingested_at
FROM {{ ref('int_products') }}
