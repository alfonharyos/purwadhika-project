import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from helpers.bigquery_helper import BigQueryProject
from dotenv import load_dotenv
import pendulum
from helpers.bigquery_helper import BigQueryProject
from helpers.discord_helper import discord_alert_message
from schemas.bigquery_schemas.taxi_trip_schemas import TAXI_ZONE_SCHEMA
from scripts.taxi_trip.script_ingest_taxi_trip import fetch_taxi_zone

# ================================ Environment Variables ========================
load_dotenv("./env")
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")


logger = LoggingMixin().log
local_tz = pendulum.timezone("Asia/Jakarta")
# ===============================================================================


# ---------- BigQuery Helpers ----------
# --- Instantiate project ---
project = BigQueryProject(
    project_id=BQ_PROJECT_ID,
    credentials_path=GOOGLE_APPLICATION_CREDENTIALS,
    location='us-central1',
    logger=logger
)

taxi_trip_raw_dataset = project.dataset("jcdeol004_alfon_taxi_trip_raw")
taxi_zone_raw_table = taxi_trip_raw_dataset.table('raw_taxi_zone')

def ensure_table():
    if not taxi_zone_raw_table.exists():
        taxi_zone_raw_table.create(
            schema=TAXI_ZONE_SCHEMA,
            run_date_bq=False
        )

def ingest_tazi_zone():
    taxi_zone_raw_table.insert_df(
        df=fetch_taxi_zone(logger),
        schema=TAXI_ZONE_SCHEMA,
        if_exists='replace',
        run_date='run_date_bq'
    )

def dag_failure_alert(context):
    dag_id = context.get("dag").dag_id
    task_instance = context.get("task_instance")
    task_id = task_instance.task_id if task_instance else "unknown"
    error = str(context.get("exception", "Unknown error"))
    discord_alert_message(
        alert_type='error',
        title=f"DAG {dag_id} failed",
        detail=f"Task {task_id} failed with error: {error}",
        logger=logger
    )

def dag_success_alert(context):
    dag_id = context.get("dag").dag_id
    run_id = context.get("run_id", "unknown")
    discord_alert_message(
        alert_type='success',
        title=f"DAG {dag_id} succeeded",
        detail=f"DAG run {run_id} completed successfully",
        logger=logger
    )

# ---------- DAG ----------
with DAG(
    dag_id="dag_ingest_taxi_zone",
    description='Initiation Taxi Zone data to BigQuery (url -> gcs -> raw)',
    tags=['init', 'taxi_trip', 'bigquery'],
    on_failure_callback=dag_failure_alert,
    on_success_callback=dag_success_alert,
    start_date=pendulum.datetime(2025, 9, 24, tz=local_tz),
    schedule_interval=None,
    catchup=False,
    is_paused_upon_creation=True
) as dag:

    ensure_task = PythonOperator(
        task_id=f"ensure_taxi_zone_table",
        python_callable=ensure_table
    )

    ingest_tazi_zone_task = PythonOperator(
        task_id=f"ingestion_taxi_zone",
        python_callable=ingest_tazi_zone
    )

    ensure_task >> ingest_tazi_zone_task
