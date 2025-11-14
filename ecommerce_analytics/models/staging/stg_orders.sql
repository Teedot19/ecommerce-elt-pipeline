
{{ config(materialized='view') }}
with src as (
SELECT
    *
FROM {{ source('bronze', 'orders_raw') }}
)



SELECT
    order_id,
    customer_id,
    product_id as order_date,
    order_date as status,
    status as total_amount,
    total_amount as shipping_fee,
    shipping_country,
    campaign,
    ingested_at
FROM src