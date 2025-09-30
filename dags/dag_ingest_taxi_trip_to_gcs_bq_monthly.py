import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.log.logging_mixin import LoggingMixin
from helpers.bigquery_helper import BigQueryProject
from helpers.gcs_helper import GCSHelper
from scripts.taxi_trip.script_ingest_taxi_trip import taxi_trip_weekend_monthly
from schemas.bigquery_schemas.taxi_trip_schemas import YELLOW_TAXI_SCHEMA, GREEN_TAXI_SCHEMA
from dotenv import load_dotenv
import pendulum
from helpers.discord_helper import discord_alert_message


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
taxi_trip_stg_dataset = project.dataset("jcdeol004_alfon_taxi_trip_stg")
taxi_trip_raw_dataset = project.dataset("jcdeol004_alfon_taxi_trip_raw")

          
def ensure_dataset():
    # ensure datasets exist
    for dataset in [taxi_trip_stg_dataset, taxi_trip_raw_dataset]:
        if not dataset.exists():
            dataset.create(location="us-central1")


def ensure_table(taxi_type: str):
    if taxi_type == "yellow":
        stg = {
            "schema": YELLOW_TAXI_SCHEMA,
            "table_name": f"stg_{taxi_type}_taxi_trip",
            "partition_field": "pickup_datetime",
            "dataset": taxi_trip_stg_dataset
        }
        raw = {
            "schema": YELLOW_TAXI_SCHEMA,
            "table_name": f"raw_{taxi_type}_taxi_trip",
            "partition_field": "pickup_datetime",
            "dataset": taxi_trip_raw_dataset
        }

    elif taxi_type == "green":
        stg = {
            "schema": GREEN_TAXI_SCHEMA,
            "table_name": f"stg_{taxi_type}_taxi_trip",
            "partition_field": "pickup_datetime",
            "dataset": taxi_trip_stg_dataset
        }
        raw = {
            "schema": GREEN_TAXI_SCHEMA,
            "table_name": f"raw_{taxi_type}_taxi_trip",
            "partition_field": "pickup_datetime",
            "dataset": taxi_trip_raw_dataset
        }

    else:
        raise ValueError(f"Unknown taxi_type: {taxi_type}")

    # create tables if not exists
    for table_cfg in [stg, raw]:
        table = table_cfg["dataset"].table(table_cfg["table_name"])
        if not table.exists():
            table.create(
                schema=table_cfg["schema"],
                partition_field=table_cfg["partition_field"],
                run_date_bq=False
            )


def ingest_task(**kwargs):
    logical_date = kwargs['logical_date']
    target_date = logical_date.subtract(months=2)
    year = target_date.year
    month = target_date.month
    return year, month

def crawl_and_upload_to_gcs(taxi_type: str, **kwargs):
    year, month = ingest_task(**kwargs)
    df = taxi_trip_weekend_monthly(year=year, month=month, taxi_type=taxi_type, logger=logger)
    df["run_date_bq"]=pendulum.now(tz=local_tz).date()
    storage.df_to_gcs(
        df=df, 
        gcs_path=f'alfon_taxi_trip/stagging/{taxi_type}/{year}/{year}_{month}_{taxi_type}_taxi.parquet', 
        file_format='parquet',
        key_columns=[
            "vendorid", "taxi_type", 
            "pickup_datetime", "dropoff_datetime",
            "pulocationid", "dolocationid",
            "trip_distance", "payment_type", "total_amount"
        ],
        md5_column='md5_key'
    )

def ingest_to_bq(taxi_type: str, **kwargs):
    year, month = ingest_task(**kwargs)
    storage.gcs_to_bq(
        gcs_path=f'alfon_taxi_trip/stagging/{taxi_type}/{year}/{year}_{month}_{taxi_type}_taxi.parquet',
        table_id=taxi_trip_stg_dataset.table(f"stg_{taxi_type}_taxi_trip").full_table_id(),
        file_format='parquet',
        write_disposition='WRITE_TRUNCATE',
        schema= YELLOW_TAXI_SCHEMA if taxi_type == 'yellow' else GREEN_TAXI_SCHEMA,
        partition_field='pickup_datetime',
        ignore_unknown_values=True,
        autodetect=False
    )

def merge_table(taxi_type: str):
    taxi_trip_raw_dataset.table(f"raw_{taxi_type}_taxi_trip")\
        .merge_from(taxi_trip_stg_dataset.table(f"stg_{taxi_type}_taxi_trip"), key_column='md5_key')


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
    dag_id="dag_ingest_taxi_trip_data_to_gcs_bq_monthly",
    description='Ingestion Taxi Trip data - store to GCS and BQ monthly: url -> GCS -> BQ (stg -> raw)',
    tags=['monthly', 'ingestion', 'taxi_trip', 'gcs', 'bigquery'],
    start_date=pendulum.datetime(2023, 3, 1, tz=local_tz), # Start ingesting Jan 2023 data (available 2 month after)
    schedule_interval='@monthly',
    catchup=True,
    max_active_runs=1,
    on_failure_callback=dag_failure_alert,
    on_success_callback=dag_success_alert,
) as dag:
    
    ensure_dataset_task = PythonOperator(
        task_id="ensure_dataset",
        python_callable=ensure_dataset
    )

    taxi_groups = []

    for taxi_type in ['yellow', 'green']:
        with TaskGroup(group_id=f'{taxi_type}_taxi_ingestion_monthly') as tg:

            ensure_table_task = PythonOperator(
                task_id=f"ensure_{taxi_type}_taxi_table",
                python_callable=ensure_table,
                op_kwargs={
                    'taxi_type': f'{taxi_type}'
                }
            )

            crawl_and_upload_task = PythonOperator(
                task_id=f"crawl_and_upload_{taxi_type}_taxi_to_gcs",
                python_callable=crawl_and_upload_to_gcs,
                op_kwargs={
                    'taxi_type': f'{taxi_type}'
                }
            )

            ingest_to_bq_task = PythonOperator(
                task_id=f"ingest_{taxi_type}_taxi_to_bq",
                python_callable=ingest_to_bq,
                op_kwargs={
                    'taxi_type': f'{taxi_type}'
                }
            )

            merge_task = PythonOperator(
                task_id=f"merge_stg_raw_{taxi_type}_taxi",
                python_callable=merge_table,
                op_kwargs={
                    'taxi_type': f'{taxi_type}'
                }
            )

            ensure_table_task >> crawl_and_upload_task >> ingest_to_bq_task >> merge_task

            taxi_groups.append(tg)
    
ensure_dataset_task >> taxi_groups

