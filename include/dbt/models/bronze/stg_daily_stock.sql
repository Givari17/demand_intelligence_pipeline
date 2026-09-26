-- Bronze: parsing tipe data dari payload JSON DAILY_STOCK.csv, tanpa
-- dedup/business rule. Grain sumber: satu baris per (product_id, date).
select
    source_record_id,   -- composite "product_id|date", lihat _ingest_csv_source
    batch_id,
    ingestion_timestamp,
    record_timestamp,
    payload ->> '$.product_id' as product_id,
    (payload ->> '$.date')::date as stock_date,
    (payload ->> '$.on_hand_qty')::integer   as on_hand_qty,
    (payload ->> '$.in_transit_qty')::integer as in_transit_qty,
    (payload ->> '$.reserved_qty')::integer  as reserved_qty
from {{ source('raw', 'bronze_daily_stock') }}
