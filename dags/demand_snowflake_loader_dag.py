"""
DAG: demand_snowflake_loader (DEMO / BACKBURNER)

Snowflake sengaja diposisikan sebagai opsi masa depan (lihat keputusan stack:
DuckDB = engine aktif fase 1, Snowflake dipelajari belakangan sebagai
third-party DWH). DAG ini cuma skeleton untuk didemokan kalau/ketika kamu
lanjut ke Snowflake -- BUKAN bagian dari jalur aktif demand_intelligence_pipeline.

is_paused_upon_creation=True dan schedule=None supaya tidak otomatis jalan
dan tidak menghabiskan trial credits Snowflake secara tidak sengaja.
Trigger manual saja: `airflow dags trigger demand_snowflake_loader`.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def _load_gold_to_snowflake(**context):
    """Baca tabel gold dari DuckDB, tulis ke Snowflake lewat dbt (target: snowflake
    di include/dbt/profiles.yml) atau lewat koneksi Airflow `snowflake_default`.
    Placeholder -- isi setelah kredensial Snowflake trial siap.
    """
    raise NotImplementedError(
        "Isi setelah kredensial Snowflake trial siap. "
        "Lihat include/dbt/profiles.yml target 'snowflake'."
    )


with DAG(
    dag_id="demand_snowflake_loader",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    tags=["supply_chain", "snowflake", "backburner", "demo-only"],
) as dag:
    PythonOperator(
        task_id="load_gold_to_snowflake",
        python_callable=_load_gold_to_snowflake,
    )
