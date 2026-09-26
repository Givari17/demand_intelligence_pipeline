-- Bronze: parsing tipe data dari payload JSON PRODUCT.csv, tanpa dedup/business rule.
select
    source_record_id as product_id,
    batch_id,
    ingestion_timestamp,
    record_timestamp,
    payload ->> '$.product_code'      as product_code,
    payload ->> '$.product_name'      as product_name,
    payload ->> '$.category'          as category,
    payload ->> '$.uom'               as uom,
    (payload ->> '$.is_active')::boolean   as is_active,
    payload ->> '$.pro_currency'      as currency,
    (payload ->> '$.base_price')::decimal  as base_price,
    (payload ->> '$.base_cost')::decimal   as base_cost,
    (payload ->> '$.holding_cost_rate')::decimal as holding_cost_rate
from {{ source('raw', 'bronze_product') }}
