-- Silver: dedup per product_id (ambil record ingestion_timestamp paling baru).
with deduped as (
    select
        *,
        row_number() over (
            partition by product_id
            order by ingestion_timestamp desc
        ) as rn
    from {{ ref('stg_product') }}
    where product_id is not null
)

select
    product_id,
    product_code,
    product_name,
    category,
    uom,
    is_active,
    currency,
    base_price,
    base_cost,
    holding_cost_rate
from deduped
where rn = 1
