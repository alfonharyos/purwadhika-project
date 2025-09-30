from airflow import DAG
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.operators.bash import BashOperator
from helpers.discord_helper import discord_alert_message
import os
import pendulum
from dotenv import load_dotenv

# --- Load ENV ---
load_dotenv("./env")
DBT_PROJECT_PATH = os.getenv("DBT_PROJECT_PATH")
DBT_PROFILES_PATH = os.getenv("DBT_PROFILES_PATH")

logger = LoggingMixin().log
local_tz = pendulum.timezone("Asia/Jakarta")

# --- Callbacks ---
def dag_failure_alert(context):
    task_instance = context.get("task_instance")
    dag_id = context.get("dag").dag_id
    task_id = task_instance.task_id if task_instance else "unknown"
    error = str(context.get("exception", "Unknown error"))
    discord_alert_message(
        alert_type="error",
        title=f"DAG {dag_id} failed",
        detail=f"Task {task_id} failed with error: {error}",
        logger=logger,
    )

def dag_success_alert(context):
    dag_id = context.get("dag").dag_id
    run_id = context.get("run_id", "unknown")
    discord_alert_message(
        alert_type="success",
        title=f"DAG {dag_id} succeeded",
        detail=f"DAG run {run_id} completed successfully",
        logger=logger,
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

# --- DAG Definition ---
with DAG(
    dag_id="dag_dbt_init_taxi_trip",
    description="DBT Init (Full Refresh) for Taxi Trip Project",
    tags=["init", "dbt", "taxi_trip", "bigquery", "full_refresh"],
    schedule_interval=None,
    start_date=pendulum.datetime(2025, 9, 30, tz=local_tz),
    catchup=False,
    on_failure_callback=dag_failure_alert,
    on_success_callback=dag_success_alert,
) as dag:

    # Tasks
    dbt_taxi_prep = make_dbt_task("dbt_init_taxi_prep", "taxi_trip", "preparation", "us_central1")
    dbt_taxi_model = make_dbt_task("dbt_init_taxi_model", "taxi_trip", "model", "us_central1")
    dbt_taxi_mart = make_dbt_task("dbt_init_taxi_mart", "taxi_trip", "mart", "us_central1")

    # Chain
    dbt_taxi_prep >> dbt_taxi_model >> dbt_taxi_mart
