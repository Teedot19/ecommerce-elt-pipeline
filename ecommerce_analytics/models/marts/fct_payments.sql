{{ config(materialized='table') }}

SELECT
    payment_date,
    payment_id,
    order_id,
    customer_id,
    payment_amount,
    payment_method,
    status
FROM {{ ref('int_payments_details') }}