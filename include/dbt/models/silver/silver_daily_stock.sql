-- Silver: dedup per (product_id, stock_date) -- grain sumbernya sendiri
-- sudah per hari per produk, dedup ini menjaga dari duplicate rerun/backfill.
with deduped as (
    select
        *,
        row_number() over (
            partition by product_id, stock_date
            order by ingestion_timestamp desc
        ) as rn
    from {{ ref('stg_daily_stock') }}
    where product_id is not null
)

select
    product_id,
    stock_date,
    on_hand_qty,
    in_transit_qty,
    reserved_qty
from deduped
where rn = 1
