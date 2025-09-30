import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime
from dotenv import load_dotenv
from helpers.postgresql_helper import execute_sql_from_file, df_to_postgres_table
from helpers.dataframe_helper import load_csv_data, add_rundate
from helpers.discord_helper import discord_alert_message

# ================================ Environment Variables ========================
load_dotenv("./envs/.env.movie_streaming")
SCHEMA_MOVIE_STREAMING = os.getenv("SCHEMA_MOVIE_STREAMING")
FILEPATH_USERS_DATA = os.getenv("FILEPATH_USERS_DATA")
FILEPATH_MOVIES_DATA = os.getenv("FILEPATH_MOVIES_DATA")
FILEPATH_PAYMENTS_DATA = os.getenv("FILEPATH_PAYMENTS_DATA")
FILEPATH_RATINGS_DATA = os.getenv("FILEPATH_RATINGS_DATA")
FILEPATH_WATCH_SESSION_DATA = os.getenv("FILEPATH_WATCH_SESSION_DATA")
FILEPATH_SUBSCRIPTIONS_DATA = os.getenv("FILEPATH_SUBSCRIPTIONS_DATA")
# ===============================================================================
logger = LoggingMixin().log

def create_movie_streaming_tables():
    execute_sql_from_file( 
        filepath=SCHEMA_MOVIE_STREAMING,
        conn_id='movie_db',
        logger=logger
    )

def insert_users_column_csv_to_postgres():
    df = load_csv_data(FILEPATH_USERS_DATA, logger=logger)
    df = add_rundate(df=df, run_date_col='run_date_pg', logger=logger)
    df_to_postgres_table(
        df = df,
        pg_table_name='users',
        pg_schema_name='public',
        mode='insert',
        conn_id='movie_db',
        logger=logger
    )

def insert_movies_column_csv_to_postgres():
    df = load_csv_data(FILEPATH_MOVIES_DATA, logger=logger)
    df = add_rundate(df=df, run_date_col='run_date_pg', logger=logger)
    df_to_postgres_table(
        df = df,
        pg_table_name='movies',
        pg_schema_name='public',
        mode='insert',
        conn_id='movie_db',
        logger=logger
    )

def insert_payments_column_csv_to_postgres():
    df = load_csv_data(FILEPATH_PAYMENTS_DATA, logger=logger)
    df = add_rundate(df=df, run_date_col='run_date_pg', logger=logger)
    df_to_postgres_table(
        df = df,
        pg_table_name='payments',
        pg_schema_name='public',
        mode='insert',
        conn_id='movie_db',
        logger=logger
    )

def insert_ratings_column_csv_to_postgres():
    df = load_csv_data(FILEPATH_RATINGS_DATA, logger=logger)
    df = add_rundate(df=df, run_date_col='run_date_pg', logger=logger)
    df_to_postgres_table(
        df = df,
        pg_table_name='ratings',
        pg_schema_name='public',
        mode='insert',
        conn_id='movie_db',
        logger=logger
    )

def insert_watch_sessions_column_csv_to_postgres():
    df = load_csv_data(FILEPATH_WATCH_SESSION_DATA, logger=logger)
    df = add_rundate(df=df, run_date_col='run_date_pg', logger=logger)
    df_to_postgres_table(
        df = df,
        pg_table_name='watch_sessions',
        pg_schema_name='public',
        mode='insert',
        conn_id='movie_db',
        logger=logger
    )

def insert_subscriptions_column_csv_to_postgres():
    df = load_csv_data(FILEPATH_SUBSCRIPTIONS_DATA, logger=logger)
    df = add_rundate(df=df, run_date_col='run_date_pg', logger=logger)
    df_to_postgres_table(
        df = df,
        pg_table_name='subscriptions',
        pg_schema_name='public',
        mode='insert',
        conn_id='movie_db',
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

with DAG(
    dag_id="dag_init_movie_streaming_data_postgres",
    description='Initiation Movie Streaming data to PostgreSQL (Create -> Insert)',
    tags=['init', 'movie_streaming', 'postgres'],
    on_failure_callback=dag_failure_alert,
    on_success_callback=dag_success_alert,
    start_date=datetime(2025, 9, 18),
    schedule_interval=None,
    catchup=False,
    is_paused_upon_creation=True
) as dag:

    create_movie_streaming_tables_task = PythonOperator(
        task_id="create_movie_streaming_tables",
        python_callable=create_movie_streaming_tables
    )

    insert_users_column_csv_to_postgres_task = PythonOperator(
        task_id="insert_users_column_csv_to_postgres",
        python_callable=insert_users_column_csv_to_postgres
    )

    insert_movies_column_csv_to_postgres_task = PythonOperator(
        task_id="insert_movies_column_csv_to_postgres",
        python_callable=insert_movies_column_csv_to_postgres
    )

    insert_payments_column_csv_to_postgres_task = PythonOperator(
        task_id="insert_payments_column_csv_to_postgres",
        python_callable=insert_payments_column_csv_to_postgres
    )

    insert_ratings_column_csv_to_postgres_task = PythonOperator(
        task_id="insert_ratings_column_csv_to_postgres",
        python_callable=insert_ratings_column_csv_to_postgres
    )

    insert_watch_sessions_column_csv_to_postgres_task = PythonOperator(
        task_id="insert_watch_sessions_column_csv_to_postgres",
        python_callable=insert_watch_sessions_column_csv_to_postgres
    )

    insert_subscriptions_column_csv_to_postgres_task = PythonOperator(
        task_id="insert_subscriptions_column_csv_to_postgres",
        python_callable=insert_subscriptions_column_csv_to_postgres
    )

    # users → parent for subscriptions, ratings, watch_sessions, payments
    # movies → parent for ratings, watch_sessions
    # subscriptions → parent for payments

    create_movie_streaming_tables_task >> [insert_users_column_csv_to_postgres_task,
                                       insert_movies_column_csv_to_postgres_task]

    insert_users_column_csv_to_postgres_task >> insert_subscriptions_column_csv_to_postgres_task

    insert_subscriptions_column_csv_to_postgres_task >> insert_payments_column_csv_to_postgres_task

    [insert_users_column_csv_to_postgres_task, insert_movies_column_csv_to_postgres_task] \
        >> insert_ratings_column_csv_to_postgres_task

    [insert_users_column_csv_to_postgres_task, insert_movies_column_csv_to_postgres_task] \
        >> insert_watch_sessions_column_csv_to_postgres_task

