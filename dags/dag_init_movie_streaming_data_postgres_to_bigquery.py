import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime, timedelta
from dotenv import load_dotenv

from helpers.postgresql_helper import extract_postgres_to_df, get_postgres_to_bq_schema, align_df_to_bq_schema
from helpers.bigquery_helper import BigQueryProject
from helpers.discord_helper import discord_alert_message

# ================================ Environment Variables ========================
load_dotenv("./env")
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
# ===============================================================================

logger = LoggingMixin().log

# ---------- BigQuery Helpers ----------
# --- Instantiate project ---
project = BigQueryProject(
    project_id=BQ_PROJECT_ID,
    credentials_path=GOOGLE_APPLICATION_CREDENTIALS,
    location='US',
    logger=logger
)

# --- Get a dataset wrapper ---
movie_stg_dataset = project.dataset("jcdeol004_alfon_movie_streaming_stg")
movie_raw_dataset = project.dataset("jcdeol004_alfon_movie_streaming_raw")

# --- Create dataset ---
for dataset in [movie_stg_dataset, movie_raw_dataset]:
    if not dataset.exists():
        dataset.create(location="US")

# ---------- Table Configuration ----------
TABLE_CONFIG = {
    'users': {
        'partition_field': 'created_at', 
        'key_column': 'user_id'
        },
    'movies': {
        'partition_field': 'created_at', 
        'key_column': 'movie_id'
        },
    'subscriptions': {
        'partition_field': 'created_at', 
        'key_column': 'subscription_id'
        },
    'payments': {
        'partition_field': 'created_at', 
        'key_column': 'payment_id'
        },
    'ratings': {
        'partition_field': 'created_at', 
        'key_column': 'rating_id'
        },
    'watch_sessions': {
        'partition_field': 'created_at', 
        'key_column': 'session_id'
        },
}

# ---------- Python Callables ----------
def ensure_bq_table(pg_table_name: str, stg_table_name: str, raw_table_name: str, partition_field: str, pg_conn: str = "movie_db", **kwargs):
    schema = get_postgres_to_bq_schema(pg_table_name, conn_id=pg_conn, logger=logger)
    for table in [movie_stg_dataset.table(stg_table_name), movie_raw_dataset.table(raw_table_name)]:
        if not table.exists():
            table.create(schema=schema, partition_field=partition_field)

def ingestion_pg_to_bq(pg_table_name: str, stg_table_name: str, partition_field: str, pg_conn: str = "movie_db", **kwargs):
    # 1. Extract from Postgres
    execution_date = kwargs['execution_date']
    end_date = execution_date - timedelta(days=1)
    df = extract_postgres_to_df(
        query = f"""
            SELECT * FROM {pg_table_name}
            WHERE DATE(created_at) < DATE('{end_date}')
        """,
        conn_id=pg_conn,
        logger=logger
    )
    schema = get_postgres_to_bq_schema(pg_table_name, conn_id=pg_conn, logger=logger)
    df = align_df_to_bq_schema(df=df, schema=schema, logger=logger)

    # 2. Insert to staging
    movie_stg_dataset.table(stg_table_name).insert_df(df=df, schema=schema, partition_field=partition_field, if_exists="replace", run_date='run_date_bq')

def upsert_raw_from_stg(stg_table_name: str, raw_table_name: str, key_column: str, **kwargs):
    movie_raw_dataset.table(raw_table_name).merge_from(
        staging_table=movie_stg_dataset.table(stg_table_name),
        key_column=key_column
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

# ---------- DAG ----------
with DAG(
    dag_id="dag_init_movie_streaming_data_pg_to_bq",
    description='Initiation Movie Streaming data to BigQuery (stg -> raw)',
    tags=['init', 'movie_streaming', 'bigquery'],
    on_failure_callback=dag_failure_alert,
    on_success_callback=dag_success_alert,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    is_paused_upon_creation=True
) as dag:

    for table_name, config in TABLE_CONFIG.items():
        with TaskGroup(group_id=table_name) as tg:
            ensure_task = PythonOperator(
                task_id=f"ensure_bq_table_{table_name}",
                python_callable=ensure_bq_table,
                op_kwargs={
                    "pg_table_name": table_name,
                    "stg_table_name": f'stg_{table_name}',
                    "raw_table_name": f'raw_{table_name}',
                    "partition_field": config['partition_field']
                },
            )

            ingestion_task = PythonOperator(
                task_id=f"ingestion_{table_name}",
                python_callable=ingestion_pg_to_bq,
                op_kwargs={
                    "pg_table_name": table_name,
                    "stg_table_name": f'stg_{table_name}',
                    "partition_field": config['partition_field']
                },
            )

            upsert_task = PythonOperator(
                task_id=f"upsert_data_{table_name}",
                python_callable=upsert_raw_from_stg,
                op_kwargs={
                    "stg_table_name": f'stg_{table_name}',
                    "raw_table_name": f'raw_{table_name}',
                    "key_column": config['key_column']
                },
            )

            ensure_task >> ingestion_task >> upsert_task
