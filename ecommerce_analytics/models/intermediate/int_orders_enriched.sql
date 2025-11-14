{{ config(materialized='table') }}


WITH filtered_orders AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        status,
        total_amount,
        campaign
    FROM {{ ref('stg_orders') }}
    WHERE total_amount > 0
),

customers AS (
    SELECT
        customer_id,
        full_name AS customer_name,
        customer_country
    FROM {{ ref('stg_customers') }}
),

final AS (
    SELECT
        c.customer_id,
        c.customer_name,
        COALESCE(c.customer_country, 'unknown') AS customer_country,
        o.order_id,
        o.order_date,
        o.status,
        o.total_amount,
        o.campaign
    FROM filtered_orders o
    JOIN customers c
        ON o.customer_id = c.customer_id
)

SELECT *
FROM final
