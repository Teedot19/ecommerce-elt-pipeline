{{ config(materialized='table') }}

SELECT
    customer_id,
    total_orders,
    total_order_amount,
    avg_order_value,
    first_order_date,
    latest_order_date
FROM {{ ref('int_customer_orders') }}
