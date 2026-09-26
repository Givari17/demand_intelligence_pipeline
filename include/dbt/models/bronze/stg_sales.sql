-- Bronze: parsing tipe data dari payload JSON SALES.csv (outbound demand),
-- tanpa dedup/business rule.
select
    source_record_id as sales_id,
    batch_id,
    ingestion_timestamp,
    record_timestamp,
    payload ->> '$.product_id'  as product_id,
    payload ->> '$.customer_id' as customer_id,
    payload ->> '$.channel_id'  as channel_id,
    (payload ->> '$.order_date')::date as order_date,
    (payload ->> '$.qty')::integer          as qty,
    (payload ->> '$.unit_price')::decimal   as unit_price,
    (payload ->> '$.discount_rate')::decimal as discount_rate,
    payload ->> '$.sale_currency' as sale_currency
from {{ source('raw', 'bronze_sales') }}
