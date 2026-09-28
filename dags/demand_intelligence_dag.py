"""
DAG: demand_intelligence_pipeline

Orkestrasi ingestion + transform, medallion architecture (bronze -> silver ->
gold), engine utama DuckDB, transform via dbt (lihat include/dbt/). Soda
dipanggil sebagai task DQ terpisah setelah gold selesai.

Domain: supply-chain analytics (demand forecasting & inventory optimization)
dari dataset sintetis 13-tabel (PRODUCT, PURCHASING, SALES, DAILY_STOCK
diingest di fase ini; 9 tabel lain ditunda) + video & komentar pasar dari
YouTube API (willingness to pay, sentimen tren) sebagai konteks pelengkap
STANDALONE -- tidak di-join ke product_id karena sales bersifat sintetis.
Model dbt di include/dbt/models/ (dim_product, fact_sales, fact_purchasing,
fact_daily_stock, fact_youtube_videos, fact_youtube_comments) sesuai domain ini.

Idempotency untuk backfill-safe:
- Tiap task ingestion menulis ke partisi berbasis execution_date (bukan append
  tanpa batas), lalu melakukan delete-then-insert pada partisi tsb sebelum load
  ulang. Ini mencegah duplicate record saat DAG di-rerun/di-backfill (PRD 7.1, 12).
- run_id memakai Airflow run_id bawaan untuk lineage/traceability (PRD 14).

Metadata pipeline run (PRD 12): pipeline_name, run_id, start_time, end_time,
status, records_processed, records_failed, error_message -> dicatat ke tabel
kontrol `pipeline_runs` di DuckDB lewat callback per task.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

DEFAULT_ARGS = {
    "owner": "data-team",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

PIPELINE_NAME = "demand_intelligence_pipeline"
DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "/opt/airflow/include/dbt/demand_intelligence.duckdb")
DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/include/dbt")


def _log_run_metadata(status: str, records_processed: int = 0,
                       records_failed: int = 0, error_message: str = None, **context):
    """Tulis satu baris ke control table `pipeline_runs` di DuckDB.
    Dipanggil per task (bukan cuma di akhir DAG) supaya kegagalan parsial
    tetap tercatat granular -- lihat _on_failure_callback.
    """
    import duckdb

    ti = context["task_instance"]
    con = duckdb.connect(DUCKDB_PATH)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            pipeline_name VARCHAR, run_id VARCHAR, task_id VARCHAR,
            start_time TIMESTAMP, end_time TIMESTAMP, status VARCHAR,
            records_processed INTEGER, records_failed INTEGER, error_message VARCHAR
        )
        """
    )
    con.execute(
        """
        INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            PIPELINE_NAME, context["run_id"], ti.task_id,
            ti.start_date, ti.end_date or datetime.utcnow(),
            status, records_processed, records_failed, error_message,
        ],
    )
    con.close()


def _on_failure_callback(context):
    exception = context.get("exception")
    _log_run_metadata(status="failed", error_message=str(exception), **context)


DATA_DIR = os.environ.get("DATA_DIR", "/opt/airflow/include/data")

# source_name -> (nama file CSV, kolom id [str tunggal atau tuple utk composite
# key], kolom timestamp bisnis atau None). Hanya 4 tabel inti dari dataset
# supply-chain 13-tabel yang diingest untuk fase ini; sisanya (supplier,
# product_supplier, supply_route, customer_company, sales_channel,
# abc_segment, calendar, special_events, weather) sengaja ditunda.
CSV_SOURCES = {
    "product":     ("PRODUCT.csv",     "product_id",           None),
    "purchasing":  ("PURCHASING.csv",  "po_line_id",           "order_date"),
    "sales":       ("SALES.csv",       "sales_id",             "order_date"),
    "daily_stock": ("DAILY_STOCK.csv", ("product_id", "date"), "date"),
}


def _ingest_csv_source(source_name: str, **context):
    """Baca CSV mentah dari DATA_DIR, tulis ke bronze_<source_name> tanpa
    transformasi (semua kolom disimpan sebagai string di payload JSON --
    pembersihan/typing terjadi di silver, bukan di sini). Delete-then-insert
    pada partisi execution_date untuk idempotency (backfill-safe, PRD 7.1, 12).
    """
    import json
    import pandas as pd
    import duckdb

    filename, id_col, ts_col = CSV_SOURCES[source_name]
    execution_date = context["ds"]
    batch_id = context["run_id"]
    now = datetime.utcnow()

    path = os.path.join(DATA_DIR, filename)
    df = pd.read_csv(path, dtype=str)

    con = duckdb.connect(DUCKDB_PATH)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS bronze_{source_name} (
            source_name VARCHAR, ingestion_timestamp TIMESTAMP,
            record_timestamp TIMESTAMP, batch_id VARCHAR, source_record_id VARCHAR,
            partition_date VARCHAR, payload JSON
        )
    """)
    con.execute(
        f"DELETE FROM bronze_{source_name} WHERE partition_date = ?",
        [execution_date],
    )

    rows = []
    for _, row in df.iterrows():
        record = row.to_dict()
        if isinstance(id_col, tuple):
            record_id = "|".join(str(record.get(c)) for c in id_col)
        else:
            record_id = str(record.get(id_col))
        rows.append((
            source_name,
            now,
            record.get(ts_col) if ts_col else now,
            batch_id,
            record_id,
            execution_date,
            json.dumps(record),
        ))

    con.executemany(
        f"INSERT INTO bronze_{source_name} VALUES (?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    con.close()

    _log_run_metadata(status="success", records_processed=len(rows), **context)


YOUTUBE_TOPICS_FILE = "youtube_topics.csv"
YOUTUBE_MAX_VIDEOS_PER_TOPIC = 10
YOUTUBE_MAX_COMMENTS_PER_VIDEO = 20


def _ingest_youtube_market_sentiment(**context):
    """Ambil video + komentar pasar (willingness to pay, sentimen tren) dari
    YouTube Data API v3 sebagai konteks pelengkap STANDALONE -- tidak di-join
    ke product_id (dataset supply-chain sintetis, jadi tidak ada hubungan
    kausal dengan sales). Topik/keyword dibaca dari include/data/youtube_topics.csv
    (kolom: topic_id, query, language).

    Menulis ke dua tabel bronze: bronze_youtube_videos (grain video) dan
    bronze_youtube_comments (grain komentar), delete-then-insert per partisi
    execution_date untuk idempotency. Butuh env YOUTUBE_API_KEY.
    Kuota: search.list = 100 unit/panggilan (kuota harian default 10.000).
    """
    import json
    import duckdb
    import pandas as pd
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError

    api_key = os.environ.get("YOUTUBE_API_KEY")
    if not api_key:
        raise RuntimeError("YOUTUBE_API_KEY belum di-set (Codespaces secret / .env).")

    execution_date = context["ds"]
    batch_id = context["run_id"]
    now = datetime.utcnow()

    topics = pd.read_csv(os.path.join(DATA_DIR, YOUTUBE_TOPICS_FILE), dtype=str)
    yt = build("youtube", "v3", developerKey=api_key, cache_discovery=False)

    video_rows, comment_rows = [], []
    seen_videos = set()

    for _, t in topics.iterrows():
        search = yt.search().list(
            part="id", q=t["query"], type="video",
            maxResults=YOUTUBE_MAX_VIDEOS_PER_TOPIC,
            relevanceLanguage=t.get("language") or None,
        ).execute()
        ids = [i["id"]["videoId"] for i in search.get("items", [])
               if i["id"]["videoId"] not in seen_videos]
        if not ids:
            continue
        seen_videos.update(ids)

        details = yt.videos().list(part="snippet,statistics", id=",".join(ids)).execute()
        for v in details.get("items", []):
            sn, st = v["snippet"], v.get("statistics", {})
            record = {
                "video_id": v["id"], "topic_id": t["topic_id"], "query": t["query"],
                "channel_title": sn.get("channelTitle"), "title": sn.get("title"),
                "published_at": sn.get("publishedAt"),
                "view_count": st.get("viewCount"), "like_count": st.get("likeCount"),
                "comment_count": st.get("commentCount"),
            }
            video_rows.append((
                "youtube_videos", now, sn.get("publishedAt"), batch_id,
                v["id"], execution_date, json.dumps(record),
            ))

            try:
                threads = yt.commentThreads().list(
                    part="snippet", videoId=v["id"], order="relevance",
                    maxResults=YOUTUBE_MAX_COMMENTS_PER_VIDEO, textFormat="plainText",
                ).execute()
            except HttpError:
                continue  # komentar dinonaktifkan / dibatasi -- lewati video ini
            for th in threads.get("items", []):
                c = th["snippet"]["topLevelComment"]
                cs = c["snippet"]
                crecord = {
                    "comment_id": c["id"], "video_id": v["id"], "topic_id": t["topic_id"],
                    "text": cs.get("textDisplay"), "like_count": cs.get("likeCount"),
                    "published_at": cs.get("publishedAt"),
                }
                comment_rows.append((
                    "youtube_comments", now, cs.get("publishedAt"), batch_id,
                    c["id"], execution_date, json.dumps(crecord),
                ))

    con = duckdb.connect(DUCKDB_PATH)
    for table, rows in (("youtube_videos", video_rows), ("youtube_comments", comment_rows)):
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS bronze_{table} (
                source_name VARCHAR, ingestion_timestamp TIMESTAMP,
                record_timestamp TIMESTAMP, batch_id VARCHAR, source_record_id VARCHAR,
                partition_date VARCHAR, payload JSON
            )
        """)
        con.execute(f"DELETE FROM bronze_{table} WHERE partition_date = ?", [execution_date])
        if rows:
            con.executemany(f"INSERT INTO bronze_{table} VALUES (?, ?, ?, ?, ?, ?, ?)", rows)
    con.close()

    _log_run_metadata(
        status="success", records_processed=len(video_rows) + len(comment_rows), **context
    )


def _run_dbt(select_tag: str, **context):
    import subprocess

    result = subprocess.run(
        ["dbt", "run", "--select", f"tag:{select_tag}", "--profiles-dir", DBT_PROJECT_DIR],
        cwd=DBT_PROJECT_DIR,
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr[-2000:])
    _log_run_metadata(status="success", **context)


def _run_soda_scan(**context):
    import subprocess

    result = subprocess.run(
        [
            "soda", "scan",
            "-d", "demand_intelligence_duckdb",
            "-c", "/opt/airflow/soda/configuration.yml",
            "/opt/airflow/soda/checks/checks_gold.yml",
        ],
        capture_output=True, text=True,
    )
    # returncode 0 = pass, 1 = warn (tetap lanjut), 2 = fail/error (blokir pipeline)
    if result.returncode >= 2:
        raise RuntimeError(result.stdout[-2000:])
    _log_run_metadata(status="success", **context)


with DAG(
    dag_id=PIPELINE_NAME,
    default_args=DEFAULT_ARGS,
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,       # jalankan backfill manual (astro/airflow dags backfill) saat butuh historical run
    max_active_runs=1,   # cegah run tumpang tindih pada file DuckDB yang sama
    tags=["supply_chain", "demand_forecasting", "youtube", "medallion", "duckdb"],
) as dag:

    with TaskGroup("ingestion") as ingestion:
        for source_name in CSV_SOURCES:
            PythonOperator(
                task_id=f"ingest_{source_name}",
                python_callable=_ingest_csv_source,
                op_kwargs={"source_name": source_name},
                on_failure_callback=_on_failure_callback,
            )
        PythonOperator(
            task_id="ingest_youtube_market_sentiment",
            python_callable=_ingest_youtube_market_sentiment,
            on_failure_callback=_on_failure_callback,
        )

    with TaskGroup("transform") as transform:
        bronze_to_silver = PythonOperator(
            task_id="bronze_to_silver",
            python_callable=_run_dbt,
            op_kwargs={"select_tag": "silver"},
            on_failure_callback=_on_failure_callback,
        )
        silver_to_gold = PythonOperator(
            task_id="silver_to_gold",
            python_callable=_run_dbt,
            op_kwargs={"select_tag": "gold"},
            on_failure_callback=_on_failure_callback,
        )
        bronze_to_silver >> silver_to_gold

    dq_checks = PythonOperator(
        task_id="soda_data_quality_checks",
        python_callable=_run_soda_scan,
        on_failure_callback=_on_failure_callback,
    )

    ingestion >> transform >> dq_checks
