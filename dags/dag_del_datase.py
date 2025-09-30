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

import os
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from dotenv import load_dotenv
from helpers.bigquery_helper import BigQueryProject

# ================================ Env ================================
load_dotenv("./env")
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
BQ_DATASET_ID = os.getenv("BQ_DATASET_ID")  
# ====================================================================

logger = LoggingMixin().log
local_tz = pendulum.timezone("Asia/Jakarta")

# ---------- BigQuery Helpers ----------
project = BigQueryProject(
    project_id=BQ_PROJECT_ID,
    credentials_path=GOOGLE_APPLICATION_CREDENTIALS,
    location="us-central1",
    logger=logger
)

# ---------- Dataset List (declare langsung di code) ----------
DATASET_LIST = [
    "jcdeol004_alfon_default_jcdeol004_alfon_taxi_trip_mart",
    "jcdeol004_alfon_default_jcdeol004_alfon_taxi_trip_model",
    "jcdeol004_alfon_default",
    "jcdeol004_alfon_default_jcdeol004_alfon_taxi_trip_preparation",
]

def delete_dataset(dataset_id: str):
    dataset_ref = f"{BQ_PROJECT_ID}.{dataset_id}"
    try:
        client = project.client
        client.delete_dataset(
            dataset_ref,
            delete_contents=True,
            not_found_ok=True
        )
        logger.info(f"[SUCCESS] Dataset {dataset_ref} deleted successfully.")
    except Exception as e:
        logger.error(f"[ERROR] Failed to delete dataset {dataset_ref}: {e}")
        raise

# ---------- DAG Definition ----------
with DAG(
    dag_id="delete_bq_datasets_dag",
    schedule_interval=None,  # run manual
    start_date=pendulum.datetime(2025, 1, 1, tz=local_tz),
    catchup=False,
    tags=["bigquery", "delete", "maintenance"],
) as dag:

    for dataset in DATASET_LIST:
        PythonOperator(
            task_id=f"delete_dataset_{dataset}",
            python_callable=delete_dataset,
            op_args=[dataset],
        )