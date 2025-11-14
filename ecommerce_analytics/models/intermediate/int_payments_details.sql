{{ config(materialized='table') }}

WITH filtered_payments AS (
    SELECT
        payment_id,
        order_id,
        payment_amount,
        {{ clean_unknown('payment_method') }} as payment_method,
        payment_date
    FROM {{ ref('stg_payments') }}
    WHERE payment_amount > 0
),

orders AS (
    SELECT
        customer_id,
        order_id,
        order_date,
        status
    FROM {{ ref('stg_orders') }}
    WHERE status NOT IN ('returned', 'cancelled')
),

final AS (
    SELECT
        fp.payment_date,
        fp.payment_id,
        fp.order_id,
        o.customer_id,
        fp.payment_amount,
        fp.payment_method,
        o.status
    FROM filtered_payments fp
    JOIN orders o
        ON fp.order_id = o.order_id
)
select * from final