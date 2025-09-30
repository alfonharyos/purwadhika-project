import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.log.logging_mixin import LoggingMixin
from helpers.bigquery_helper import BigQueryProject
from helpers.gcs_helper import GCSHelper
from schemas.bigquery_schemas.putusan_ma_schema import PUTUSAN_LINK_PAGE_SCHEMA, PUTUSAN_MA_SCHEMA, PUTUSAN_EXTRACT_PDF_SCHEMA
from dotenv import load_dotenv
import pendulum
from helpers.discord_helper import discord_alert_message
from utils.putusan_ma_utils import (
    get_year_month_task,
    scrape_and_ingest_link_putusan_to_bq,
    scrape_and_ingest_putusan_detail_to_bq_and_upload_pdf_to_gcs,
    extract_ingest_upload_putusan_pdf_to_bq_gcs,
)


load_dotenv("./env")
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")


logger = LoggingMixin().log
local_tz = pendulum.timezone("Asia/Jakarta")

# --- Instantiate project ---
project = BigQueryProject(
    project_id=BQ_PROJECT_ID,
    credentials_path=GOOGLE_APPLICATION_CREDENTIALS,
    location='us-central1',
    logger=logger
)
# --- Instantiate gcp ---
storage = GCSHelper(
    bucket_name=GCS_BUCKET_NAME,
    project_id=project.project_id,
    credentials_path=GOOGLE_APPLICATION_CREDENTIALS,
    logger=logger
)
# --- Instantiate dataset ---
putusan_ma_stg = project.dataset("jcdeol004_alfon_putusan_ma_stg")
putusan_ma_raw = project.dataset("jcdeol004_alfon_putusan_ma_raw")

stg_putusan_link_page_table = putusan_ma_stg.table('stg_putusan_link_page')
stg_putusan_table = putusan_ma_stg.table('stg_putusan_ma')
stg_putusan_extract_pdf_table = putusan_ma_stg.table('stg_putusan_extract_pdf')

raw_putusan_table = putusan_ma_raw.table('raw_putusan_ma')
raw_putusan_extract_pdf_table = putusan_ma_raw.table('raw_putusan_extract_pdf')
# ---------------------------

          
def ensure_dataset():
    # ensure datasets exist
    for dataset in [putusan_ma_stg, putusan_ma_raw]:
        if not dataset.exists():
            dataset.create(location="us-central1")

def ensure_table():
    table_configs = [
        (stg_putusan_link_page_table, PUTUSAN_LINK_PAGE_SCHEMA),
        (stg_putusan_table, PUTUSAN_MA_SCHEMA),
        (stg_putusan_extract_pdf_table, PUTUSAN_EXTRACT_PDF_SCHEMA),
        (raw_putusan_table, PUTUSAN_MA_SCHEMA),
        (raw_putusan_extract_pdf_table, PUTUSAN_EXTRACT_PDF_SCHEMA),
    ]
    for table, schema in table_configs:
        if not table.exists():
            table.create(
                schema=schema,
                partition_field="tgl_putusan",
                run_date_bq=False
            )

def merge_raw_from_stg():
    logger.info("Merging stg_putusan_table -> raw_putusan_table")
    raw_putusan_table.merge_from(
        staging_table=stg_putusan_table, 
        key_column='no_putusan'
    )
    logger.info("Merging stg_putusan_extract_pdf_table -> raw_putusan_extract_pdf_table")
    raw_putusan_extract_pdf_table.merge_from(
        staging_table=stg_putusan_extract_pdf_table, 
        key_column='no_putusan'
    )

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

with DAG(
    dag_id="dag_scraping_putusan_ma_bq_gcs_monthly",
    description='Scraping Putusan MA data - store to GCS and BQ monthly: url -> GCS -> BQ (stg -> raw)',
    tags=['monthly', 'scraping', 'ingestion', 'putusan MA', 'gcs', 'bigquery'],
    start_date=pendulum.datetime(2024, 11, 1, tz=local_tz),
    end_date=pendulum.datetime(2025, 3, 1, tz=local_tz),
    schedule_interval="@monthly",
    catchup=True,
    max_active_runs=1,
    on_failure_callback=dag_failure_alert,
    on_success_callback=dag_success_alert,
) as dag:
    
    ensure_dataset_task = PythonOperator(
        task_id="ensure_dataset",
        python_callable=ensure_dataset
    )
    
    ensure_table_task = PythonOperator(
        task_id="ensure_table",
        python_callable=ensure_table
    )

    scrape_link_task = PythonOperator(
        task_id="scrape_link",
        python_callable=lambda **kwargs: scrape_and_ingest_link_putusan_to_bq(
            *get_year_month_task(**kwargs), logger, stg_putusan_link_page_table
        )
    )

    scrape_detail_task = PythonOperator(
        task_id="scrape_detail",
        python_callable=lambda **kwargs: scrape_and_ingest_putusan_detail_to_bq_and_upload_pdf_to_gcs(
            *get_year_month_task(**kwargs), logger, stg_putusan_link_page_table, stg_putusan_table, storage
        )
    )

    extract_pdf_task = PythonOperator(
        task_id="extract_pdf",
        python_callable=lambda **kwargs: extract_ingest_upload_putusan_pdf_to_bq_gcs(
            *get_year_month_task(**kwargs), logger, stg_putusan_table, stg_putusan_extract_pdf_table, storage
        )
    )
 
    merge_raw_from_stg_task = PythonOperator(
        task_id="merge_raw_from_stg",
        python_callable=merge_raw_from_stg
    )
    
ensure_dataset_task >> ensure_table_task \
    >> scrape_link_task >> scrape_detail_task >> extract_pdf_task \
                >> merge_raw_from_stg_task
