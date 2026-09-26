-- Grain: satu baris per po_line_id (satu baris purchase order line).
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
    qty_ordered * unit_cost as total_cost
from {{ ref('silver_purchasing') }}
