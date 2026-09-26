-- Silver: dedup per po_line_id, filter baris tanpa product_id (completeness
-- dasar, detail di Soda).
with deduped as (
    select
        *,
        row_number() over (
            partition by po_line_id
            order by ingestion_timestamp desc
        ) as rn
    from {{ ref('stg_purchasing') }}
)

select
    po_line_id,
    product_id,
    supplier_id,
    route_id,
    order_date,
    receipt_date,
    qty_ordered,
    qty_received,
    unit_cost,
    purch_currency
from deduped
where rn = 1
  and product_id is not null
