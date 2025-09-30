from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from google.cloud import bigquery
from helpers.bigquery_helper import BigQueryProject
from helpers.discord_helper import discord_alert_message, discord_send_message
from scripts.pinjol.crawl_pinjol_statistics import get_stat_adapundi
import pendulum
import os
import pandas as pd
from dotenv import load_dotenv

# ================================ Environment Variables ========================
load_dotenv("./env")
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
# ===============================================================================

logger = LoggingMixin().log

local_tz = pendulum.timezone("Asia/Jakarta")

# ---------- BigQuery Helpers ----------
# --- Instantiate project ---
project = BigQueryProject(
    project_id=BQ_PROJECT_ID,
    credentials_path=GOOGLE_APPLICATION_CREDENTIALS,
    location='us-central1',
    logger=logger
)

ds_pinjol = project.dataset('jcdeol004_alfon_pinjol_source')
table_pinjol = ds_pinjol.table('src_adapundi')
schema=[
    bigquery.SchemaField("penerima_dana_total", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("penerima_dana_tahun_berjalan", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("penerima_dana_posisi_akhir", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("pemberi_dana_total", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("pemberi_dana_tahun_berjalan", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("pemberi_dana_posisi_akhir", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("dana_tersalurkan_total", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("dana_tersalurkan_tahun_berjalan", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("dana_tersalurkan_posisi_akhir", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("run_date_bq", "DATE", mode="REQUIRED")
]

def ensure_dataset_and_table():
    if not ds_pinjol.exists():
        ds_pinjol.create(location="us-central1")

    if not table_pinjol.exists():
        table_pinjol.create(
            schema=schema,
            partition_field="run_date_bq",
            run_date_bq=True
        )

# ---------- Task Functions ----------
def scrape_data(**context):
    df = get_stat_adapundi(logger)
    if df.empty:
        raise ValueError("No data scraped")

    # misalnya cuma 1 row → simpan dict ke XCom
    context['ti'].xcom_push(key="scraped_df", value=df.to_dict(orient="records"))
    logger.info("Scraping done")
    return True

def ingest_bq(**context):
    data = context['ti'].xcom_pull(task_ids="scrape_adapundi", key="scraped_df")
    if not data:
        raise ValueError("Data scrape not available")

    import pandas as pd
    df = pd.DataFrame(data)

    table_pinjol.delete_where(f"run_date_bq = '{pendulum.now(tz=local_tz).date()}'")

    table_pinjol.insert_df(
        df=df,
        schema=schema,
        partition_field="run_date_bq",
        if_exists="append",
        run_date='run_date_bq'
    )
    logger.info("Ingest to BQ done")
    return len(df)

def report_dc(**context):
    data = context['ti'].xcom_pull(task_ids="scrape_adapundi", key="scraped_df")
    if not data:
        raise ValueError("Data scrape not available for report")

    row = data[0]
    discord_send_message(
        title="Adapundi Statistik:",
        message=(
            f"date {pendulum.now(tz=local_tz).date()}"
            f"\n**Penerima Dana** - Posisi Akhir: {row['penerima_dana_posisi_akhir']:,}"
            f"\n**Pemberi Dana** - Posisi Akhir: {row['pemberi_dana_posisi_akhir']:,}"
            f"\n**Dana Tersalurkan** - Posisi Akhir: {row['dana_tersalurkan_posisi_akhir']:,}"
        ),
        logger=logger
    )
    logger.info("Report sent to Discord")
    return True



def dag_failure_alert(context):
    task_instance = context.get("task_instance")
    dag_id = context.get("dag").dag_id
    task_id = task_instance.task_id if task_instance else "unknown"
    error = str(context.get("exception"))
    discord_alert_message(
        alert_type='error',
        title=f"DAG {dag_id} failed",
        detail=f"Task {task_id} failed with error: {error}",
        logger=logger
    )

def dag_success_alert(context):
    dag_id = context.get("dag").dag_id
    run_id = context.get("run_id")
    discord_alert_message(
        alert_type='success',
        title=f"DAG {dag_id} succeeded",
        detail=f"DAG run {run_id} completed successfully",
        logger=logger
    )

# ---------- DAG ----------
with DAG(
    dag_id="dag_ingest_to_bq_and_report_to_dc_scrape_pinjol_daily",
    description="Scrape adapundi >> ingest ke BigQuery >> report ke Discord",
    tags=["daily", "scraping", "pinjol", "bigquery", "discord"],
    schedule_interval="0 23 * * *",
    start_date=pendulum.datetime(2025, 9, 24, tz=local_tz),
    catchup=False,
    on_failure_callback=dag_failure_alert,
    on_success_callback=dag_success_alert,
) as dag:
    
    ensure_dataset_and_table_task = PythonOperator(
        task_id="ensure_dataset",
        python_callable=ensure_dataset_and_table
    )

    scrape_task = PythonOperator(
        task_id="scrape_adapundi",
        python_callable=scrape_data,
        provide_context=True,
    )

    ingest_bq_task = PythonOperator(
        task_id="ingest_to_bigquery",
        python_callable=ingest_bq,
        provide_context=True,
    )

    report_dc_task = PythonOperator(
        task_id="report_to_discord",
        python_callable=report_dc,
        provide_context=True,
    )

    ensure_dataset_and_table_task >> scrape_task >> ingest_bq_task >> report_dc_task