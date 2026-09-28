# Demand Intelligence Pipeline

Pipeline data domain **supply chain analytics** (demand forecasting &
inventory optimization) + **konteks pasar** (video & komentar YouTube tentang
willingness to pay dan sentimen tren, sebagai pelengkap standalone).

Alur: ingestion (CSV supply-chain + YouTube API) → Airflow (CeleryExecutor +
Redis) → DuckDB (bronze/silver/gold via dbt) → Soda (data quality) → Metabase
(BI). Snowflake diposisikan sebagai backburner/opsi masa depan. Prometheus +
Grafana untuk monitoring.

> Proyek ini sebelumnya bernama `marketing_medallion_pipeline` dan dibangun
> untuk domain campaign/ads marketing (CRM, spend, conversion). Data itu
> sudah tidak dipakai lagi — lihat "Riwayat perubahan scope" di bawah.

## Sumber data

| Sumber | Isi | Grain |
|---|---|---|
| `PRODUCT.csv` | Master produk — harga, cost, kategori, UoM | 1 baris / produk |
| `PURCHASING.csv` | Purchase order dari supplier (inbound supply) | 1 baris / PO line |
| `SALES.csv` | Order penjualan (outbound demand) | 1 baris / sales line |
| `DAILY_STOCK.csv` | Posisi stok harian per produk | 1 baris / (produk, tanggal) |
| YouTube API | Video & komentar pasar (willingness to pay, sentimen tren), topik dari `include/data/youtube_topics.csv` | 1 baris / video; 1 baris / komentar |

Keempat CSV berasal dari dataset supply-chain sintetis (13 tabel relasional,
seed=42, periode 2025-01-01 s/d 2025-12-31, dibuat untuk demand forecasting
& inventory optimization) — hanya 4 tabel inti yang diingest di fase ini,
9 tabel lain (SUPPLIER, PRODUCT_SUPPLIER, SUPPLY_ROUTE, CUSTOMER_COMPANY,
SALES_CHANNEL, ABC_SEGMENT_v22, CALENDAR, SPECIAL_EVENTS, WEATHER_CLEAN)
sengaja ditunda.

Data ini **historis dan statis** (satu periode tetap, bukan feed harian yang
terus mengalir) — ini memengaruhi beberapa keputusan DQ di bawah (tidak ada
check freshness/volume-anomaly seperti versi sebelumnya).

## Kenapa docker-compose resmi Airflow, bukan `astro dev start`

Astro CLI itu open-source dan gratis, tapi **local dev environment-nya
(`astro dev start`) cuma mendukung LocalExecutor** — CeleryExecutor+Redis di
Astro CLI hanya tersedia kalau deploy ke Astro Cloud (hosted, berbayar), bukan
untuk environment lokal/Codespaces. Karena kamu eksplisit mau Redis +
CeleryExecutor jalan di Codespaces, project ini pakai
[docker-compose resmi Apache Airflow](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
yang di-custom, bukan Astro CLI.

Struktur folder (`dags/`, `include/`, `plugins/`, `requirements.txt`) sengaja
tetap konvensi yang familiar, supaya gampang dipindah ke Astro CLI/Astro Cloud
nanti kalau suatu saat mau hosted deployment.

## Prasyarat di Codespaces

Codespaces sudah punya Docker + Docker Compose terpasang, jadi tidak perlu
instalasi tambahan di layer ini.

## Setup

1. Salin `.env.example` ke `.env`:
   ```bash
   cp .env.example .env
   echo "AIRFLOW_UID=$(id -u)" >> .env
   ```
   Untuk credential asli (Snowflake, YouTube API key), lebih aman set
   sebagai **Codespaces secret** (Settings → Secrets and variables →
   Codespaces) daripada ditulis di `.env` yang ikut ter-commit.

2. Taruh 4 file CSV (`PRODUCT.csv`, `PURCHASING.csv`, `SALES.csv`,
   `DAILY_STOCK.csv`) di `include/data/` — lihat `include/data/README.md`.

3. Inisialisasi database & user admin Airflow:
   ```bash
   docker compose up airflow-init
   ```

4. Jalankan semua service:
   ```bash
   docker compose up -d --build
   ```

5. Cek semua container sehat:
   ```bash
   docker compose ps
   ```

## Akses (lewat tab "Ports" di Codespaces)

| Service    | Port | Keterangan                                  |
|------------|------|------------------------------------------------|
| Airflow UI | 8080 | login `airflow` / `airflow` (default, ganti!) |
| Prometheus | 9090 | query metrik mentah                           |
| Grafana    | 3000 | login `admin` / nilai `GF_SECURITY_ADMIN_PASSWORD` |
| Metabase   | 3001 | setup wizard saat pertama buka                |

## Menjalankan dbt & Soda manual (di luar jadwal DAG)

```bash
docker compose exec airflow-scheduler bash
cd /opt/airflow/include/dbt
dbt run --select tag:silver
dbt run --select tag:gold
dbt test

soda scan -d demand_intelligence_duckdb -c /opt/airflow/soda/configuration.yml \
  /opt/airflow/soda/checks/checks_gold.yml
```

## Grafana dashboard untuk metrik Airflow

Datasource Prometheus sudah otomatis ke-provision. Belum ada dashboard JSON
bawaan di sini — cari dashboard komunitas "Airflow statsd" di
https://grafana.com/grafana/dashboards/ dan import lewat UI Grafana
(+ → Import), atau bikin panel sendiri dari metrik `airflow_*` yang sudah
masuk ke Prometheus.

## Metabase + DuckDB — perlu driver tambahan

Metabase **tidak punya driver DuckDB bawaan**. Kamu perlu pasang community
driver plugin (`metabase-duckdb-driver`) ke folder plugin Metabase sebelum
bisa connect ke file `demand_intelligence.duckdb`. Cek rilis terbaru
drivernya dulu sebelum dipasang — API driver Metabase berubah antar versi,
jadi pastikan versi driver cocok dengan versi image `metabase/metabase` yang
dipakai di `docker-compose.yml`.

## Snowflake (backburner)

`dags/demand_snowflake_loader_dag.py` sengaja `is_paused_upon_creation=True`
dan `schedule=None` supaya tidak jalan otomatis / tidak menghabiskan trial
credits. Isi task-nya masih placeholder — lengkapi setelah kredensial trial
Snowflake siap (lihat `include/dbt/profiles.yml` target `snowflake`).

## Model gold yang tersedia

- `dim_product` — master produk (1 baris/produk)
- `dim_date` — dimensi kalender generik
- `fact_sales` — sales line + `net_revenue` terhitung
- `fact_purchasing` — PO line + `total_cost` terhitung
- `fact_daily_stock` — posisi stok harian per produk
- `fact_youtube_videos` / `fact_youtube_comments` — konteks pasar
  standalone. **Sengaja tidak di-join ke `product_id`/`fact_sales`**: sales
  bersifat sintetis sehingga tidak ada hubungan kausal dengan YouTube. Cukup
  dipakai sebagai pembanding tren/sentimen, bukan analisis dampak.

## Yang masih placeholder (`TODO` di kode)

- `_ingest_youtube_market_sentiment` sudah terimplementasi (YouTube Data API
  v3) tapi **belum pernah dijalankan/diuji** — butuh `YOUTUBE_API_KEY` sebagai
  Codespaces secret, dan topik di `include/data/youtube_topics.csv` masih
  contoh awal yang perlu kamu sesuaikan.
- 9 tabel lain dari dataset supply-chain (SUPPLIER, PRODUCT_SUPPLIER,
  SUPPLY_ROUTE, CUSTOMER_COMPANY, SALES_CHANNEL, ABC_SEGMENT_v22, CALENDAR,
  SPECIAL_EVENTS, WEATHER_CLEAN) belum dipetakan ke `CSV_SOURCES` — sengaja
  ditunda.
- Mart-level metrics (mis. uplift demand pasca video review, safety-stock,
  ABC segmentation) belum dibangun — satu layer di atas gold, baru relevan
  setelah gold ini stabil.

## Riwayat perubahan scope

Proyek ini awalnya bernama `marketing_medallion_pipeline`, berdomain
campaign/ads marketing (CRM leads, campaign spend, conversion funnel).
Setelah dicek, data leads/campaign/conversion yang dimaksud ternyata tidak
pernah tersedia — yang ada adalah dataset supply-chain sintetis (product,
purchasing, sales, daily_stock) dan data pasar dari YouTube. Karena kedua
sumber ini tidak mengandung data campaign/ads sama sekali, seluruh
penamaan, dbt model (dim_campaign, dim_customer, fact_conversion,
fact_marketing_spend), dan Soda checks yang khusus untuk skema Marketing
telah diganti total ke domain supply-chain analytics + content-marketing
impact, sesuai isi README ini.
