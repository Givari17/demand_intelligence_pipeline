# Data folder

Taruh 4 file CSV berikut di folder ini (nama harus persis, termasuk huruf besar):

- `PRODUCT.csv`
- `PURCHASING.csv`
- `SALES.csv`
- `DAILY_STOCK.csv`

Sumber: dataset supply-chain sintetis (13 tabel relasional, seed=42, periode
2025-01-01 s/d 2025-12-31). Hanya 4 tabel ini yang diingest di fase saat ini --
9 tabel lain (SUPPLIER, PRODUCT_SUPPLIER, SUPPLY_ROUTE, CUSTOMER_COMPANY,
SALES_CHANNEL, ABC_SEGMENT_v22, CALENDAR, SPECIAL_EVENTS, WEATHER_CLEAN)
ditunda dan belum dipetakan ke `CSV_SOURCES` di
`dags/demand_intelligence_dag.py`.

File di folder ini sengaja di-gitignore (lihat `.gitignore` root) supaya data
mentah tidak ikut ter-commit ke repo.
