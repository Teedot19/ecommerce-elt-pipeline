WITH src as (
SELECT
  *
FROM {{ source('bronze', 'order_items_raw') }})

select
ORDER_ITEM_ID,
ORDER_ID, 
PRODUCT_ID, 
   {{ clean_numeric('UNIT_PRICE') }}   AS UNIT_PRICE,
    {{ clean_numeric('QUANTITY') }}     AS ORDER_QUANTITY, 
    {{ clean_numeric('LINE_TOTAL') }}   AS LINE_TOTAL,
INGESTED_AT
from src