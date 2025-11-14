{{ config(materialized='table') }}

SELECT
    customer_id,
    full_name,
    email,
    customer_country,
    signup_date,
    ingested_at
FROM {{ ref('int_customers') }}
