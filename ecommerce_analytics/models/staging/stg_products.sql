{{ config(materialized='view') }}
with customers as (
SELECT
*
FROM {{ source('bronze', 'products_raw') }})


select 
product_id,
name as product_name,
category as product_category,
  {{ clean_numeric('PRICE') }}   AS PRICE,
    {{ clean_numeric('STOCK_COUNT') }}  AS STOCK_COUNT,
created_at,
is_active,
ingested_at 
from customers