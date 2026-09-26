-- Grain: satu baris per tanggal kalender.
-- Pakai generate_series bawaan DuckDB supaya tidak butuh dependency dbt_utils.

select
    d::date                       as date_day,
    extract(year from d)          as year,
    extract(month from d)         as month,
    extract(day from d)           as day,
    extract(dow from d)           as day_of_week,   -- 0=Minggu .. 6=Sabtu
    extract(week from d)          as iso_week
from generate_series(
    date '2025-01-01',
    date '2027-12-31',
    interval '1 day'
) as t(d)
