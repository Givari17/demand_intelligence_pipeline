# Data folder

## CSV supply-chain (di-gitignore, upload manual ke Codespaces)

Nama file harus persis (huruf besar):

- `PRODUCT.csv`
- `PURCHASING.csv`
- `SALES.csv`
- `DAILY_STOCK.csv`

Sumber: dataset supply-chain sintetis (13 tabel relasional, seed=42, periode
2025-01-01 s/d 2025-12-31). Hanya 4 tabel ini yang diingest di fase ini; 9
tabel lain (SUPPLIER, PRODUCT_SUPPLIER, SUPPLY_ROUTE, CUSTOMER_COMPANY,
SALES_CHANNEL, ABC_SEGMENT_v22, CALENDAR, SPECIAL_EVENTS, WEATHER_CLEAN)
ditunda.

## `youtube_topics.csv` (ikut ter-commit)

Daftar topik/keyword pencarian YouTube untuk konteks pasar (willingness to
pay, sentimen tren). Kolom: `topic_id`, `query`, `language`. Edit sesuai
kebutuhan; tiap topik memakai ~100 unit kuota YouTube API (search.list),
kuota harian default 10.000.
