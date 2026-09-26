-- Grain: satu baris per product_id.
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
from {{ ref('silver_product') }}
