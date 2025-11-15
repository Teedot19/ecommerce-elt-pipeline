{{ config(materialized='view') }}

WITH src AS (
    SELECT * FROM {{ source('bronze', 'payments_raw') }}
)

SELECT
    PAYMENT_ID,
    ORDER_ID,
    {{ clean_numeric('AMOUNT') }}     AS PAYMENT_AMOUNT,
    PAYMENT_METHOD,
    PAID_AT AS PAYMENT_DATE,
    INGESTED_AT
FROM src
