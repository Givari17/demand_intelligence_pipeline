-- Silver: dedup per sales_id, filter baris tanpa product_id (completeness
-- dasar, detail di Soda).
with deduped as (
    select
        *,
        row_number() over (
            partition by sales_id
            order by ingestion_timestamp desc
        ) as rn
    from {{ ref('stg_sales') }}
)

select
    sales_id,
    product_id,
    customer_id,
    channel_id,
    order_date,
    qty,
    unit_price,
    discount_rate,
    sale_currency
from deduped
where rn = 1
  and product_id is not null
