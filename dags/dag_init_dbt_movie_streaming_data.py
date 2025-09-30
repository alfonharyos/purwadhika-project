from airflow import DAG
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from helpers.bigquery_helper import BigQueryProject
from helpers.discord_helper import discord_alert_message
import os
from dotenv import load_dotenv

load_dotenv("./env")
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
DBT_PROJECT_PATH = os.getenv("DBT_PROJECT_PATH")
DBT_PROFILES_PATH = os.getenv("DBT_PROFILES_PATH")

logger = LoggingMixin().log

# --- Instantiate project ---
project = BigQueryProject(
    project_id=BQ_PROJECT_ID,
    credentials_path=GOOGLE_APPLICATION_CREDENTIALS,
    location='US',
    logger=logger
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

def make_dbt_task(task_id: str, domain: str, tag: str, target: str):
    return BashOperator(
        task_id=task_id,
        bash_command=(
            f"cd {DBT_PROJECT_PATH} "
            f"&& dbt run --select {domain},tag:{tag} "
            f"--profiles-dir {DBT_PROFILES_PATH} --target {target} --full-refresh"
        ),
    )


with DAG(
    dag_id="dag_init_dbt_movie_streaming_data",
    description='Initiation DBT Movie Streaming ETL monthly: preparation -> model -> mart',
    tags=['init', 'dbt', 'movie_streaming', 'bigquery'],
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    is_paused_upon_creation=True,
    on_failure_callback=dag_failure_alert,
    on_success_callback=dag_success_alert,
) as dag:
    
    # Tasks
    dbt_movie_streaming_prep = make_dbt_task("dbt_init_movie_streaming_prep", "movie_streaming", "preparation", "us")
    dbt_movie_streaming_model = make_dbt_task("dbt_init_movie_streaming_model", "movie_streaming", "model", "us")
    dbt_movie_streaming_mart = make_dbt_task("dbt_init_movie_streaming_mart", "movie_streaming", "mart", "us")

    # Chain
    dbt_movie_streaming_prep >> dbt_movie_streaming_model >> dbt_movie_streaming_mart