
with src as (
    select * from {{ source('bronze', 'customers_raw') }}
)

select
      customer_id,
 CONCAT(first_name, ' ', last_name) AS full_name,
 {{ clean_email('email') }} as email,
 {{null_if_bad_country('country')}} as customer_country,
    country,
    signup_date,
  INGESTED_AT
from src




