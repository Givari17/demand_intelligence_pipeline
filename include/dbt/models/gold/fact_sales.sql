-- Grain: satu baris per sales_id (satu baris order/sales line).
-- Metric definition (PRD 10): net_revenue = qty * unit_price * (1 - discount_rate).
select
    sales_id,
    product_id,
    customer_id,
    channel_id,
    order_date,
    qty,
    unit_price,
    discount_rate,
    sale_currency,
    qty * unit_price * (1 - coalesce(discount_rate, 0)) as net_revenue
from {{ ref('silver_sales') }}
