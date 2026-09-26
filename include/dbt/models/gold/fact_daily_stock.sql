-- Grain: satu baris per (product_id, date). Basis untuk analisis stockout
-- dan safety-stock -- lihat catatan "Known limitation" dataset asli soal
-- korelasi discount_rate vs qty yang masih lemah untuk price-elasticity.
select
    product_id,
    stock_date as date_day,
    on_hand_qty,
    in_transit_qty,
    reserved_qty
from {{ ref('silver_daily_stock') }}
