-- Bronze: parsing tipe data dari payload JSON PURCHASING.csv (inbound supply),
-- tanpa dedup/business rule.
select
    source_record_id as po_line_id,
    batch_id,
    ingestion_timestamp,
    record_timestamp,
    payload ->> '$.product_id'   as product_id,
    payload ->> '$.supplier_id'  as supplier_id,
    payload ->> '$.route_id'     as route_id,
    (payload ->> '$.order_date')::date   as order_date,
    (payload ->> '$.receipt_date')::date as receipt_date,
    (payload ->> '$.qty_ordered')::integer  as qty_ordered,
    (payload ->> '$.qty_received')::integer as qty_received,
    (payload ->> '$.unit_cost')::decimal    as unit_cost,
    payload ->> '$.purch_currency' as purch_currency
from {{ source('raw', 'bronze_purchasing') }}
