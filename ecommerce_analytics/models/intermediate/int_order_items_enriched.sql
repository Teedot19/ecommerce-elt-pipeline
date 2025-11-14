{{ config(materialized='table') }}


WITH filtered_order_items AS (
    SELECT
    order_item_id,
        order_id,
        product_id,
        unit_price,
        order_quantity,
        line_total,
        ingested_at
    FROM {{ ref('stg_orders_items') }}
    WHERE order_quantity > 0
),

products AS (
    SELECT
        product_id,
        product_name,
        product_category,
        price,
        stock_count,
        created_at,
        is_active,
        ingested_at
    FROM {{ ref('stg_products') }}
),

final AS (
    SELECT

        o.order_id,
        o.order_item_id,
        p.product_id,
        o.unit_price,
        o.order_quantity,
        p.product_name,
        p.product_category
    FROM filtered_order_items o
    JOIN products p
        ON o.product_id = p.product_id
)

SELECT *
FROM final
