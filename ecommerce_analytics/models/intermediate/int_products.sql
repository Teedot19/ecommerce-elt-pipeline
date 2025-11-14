{{ config(materialized='table') }}

WITH src AS (
    SELECT * FROM {{ ref('stg_products') }}
)

SELECT
    PRODUCT_ID,
    PRODUCT_NAME,
    PRODUCT_CATEGORY,
    {{ clean_numeric('PRICE') }}        AS PRICE,
    {{ clean_numeric('STOCK_COUNT') }}  AS STOCK_COUNT,
    CREATED_AT,
    IS_ACTIVE,
    INGESTED_AT
FROM src
