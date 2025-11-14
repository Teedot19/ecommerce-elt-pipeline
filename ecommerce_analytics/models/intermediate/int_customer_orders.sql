{{ config(materialized='table') }}



WITH orders AS (
    SELECT
        customer_id,
        order_id,
        order_date,
        total_amount
    FROM {{ ref('stg_orders') }}
    WHERE total_amount > 0
),

payments AS (
    SELECT
        order_id,
        payment_amount
    FROM {{ ref('stg_payments') }}
),

order_payments AS (
    SELECT
        o.customer_id,
        o.order_id,
        o.order_date,
        o.total_amount,
        COALESCE(p.payment_amount, 0) AS payment_amount
    FROM orders o
    LEFT JOIN payments p
        ON o.order_id = p.order_id
),

final AS (
    SELECT
        customer_id,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(total_amount) AS total_order_amount,
        SUM(total_amount)/COUNT(DISTINCT order_id) AS avg_order_value,
        MIN(order_date) AS first_order_date,
        MAX(order_date) AS latest_order_date
    FROM order_payments
    GROUP BY customer_id
)

SELECT *
FROM final