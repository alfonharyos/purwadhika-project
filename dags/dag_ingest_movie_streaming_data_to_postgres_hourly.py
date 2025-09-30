import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime
from helpers.postgresql_helper import df_to_postgres_table
from helpers.dataframe_helper import add_rundate
from scripts.movie_streaming.movie_streaming_data_generator import generate_movie_streaming_data
from helpers.discord_helper import discord_alert_message
import pendulum

logger = LoggingMixin().log
local_tz = pendulum.timezone("Asia/Jakarta")

def generate_movie_streaming_data_hourly(**context):
    data = generate_movie_streaming_data()
    context['ti'].xcom_push(key='movie_streaming_data', value=data)

def upsert_users_column_df_to_postgres(**context):
    data = context['ti']\
        .xcom_pull(key='movie_streaming_data', task_ids='generate_movie_streaming_data')
    df_users = add_rundate(df=data['users'], run_date_col='run_date_pg', logger=logger)
    df_to_postgres_table(
        df = df_users,
        pg_table_name='users',
        pg_schema_name='public',
        mode='upsert',
        conn_id='movie_db',
        logger=logger
    )

def upsert_movies_column_df_to_postgres(**context):
    data = context['ti']\
        .xcom_pull(key='movie_streaming_data', task_ids='generate_movie_streaming_data')
    df_movies = add_rundate(df=data['movies'], run_date_col='run_date_pg', logger=logger)
    df_to_postgres_table(
        df = df_movies,
        pg_table_name='movies',
        pg_schema_name='public',
        mode='upsert',
        conn_id='movie_db',
        logger=logger
    )

def upsert_payments_column_df_to_postgres(**context):
    data = context['ti']\
        .xcom_pull(key='movie_streaming_data', task_ids='generate_movie_streaming_data')
    df_payments = add_rundate(df=data['payments'], run_date_col='run_date_pg', logger=logger)
    df_to_postgres_table(
        df = df_payments,
        pg_table_name='payments',
        pg_schema_name='public',
        mode='upsert',
        conn_id='movie_db',
        logger=logger
    )

def upsert_ratings_column_df_to_postgres(**context):
    data = context['ti']\
        .xcom_pull(key='movie_streaming_data', task_ids='generate_movie_streaming_data')
    df_ratings = add_rundate(df=data['ratings'], run_date_col='run_date_pg', logger=logger)
    df_to_postgres_table(
        df = df_ratings,
        pg_table_name='ratings',
        pg_schema_name='public',
        mode='upsert',
        conn_id='movie_db',
        logger=logger
    )

def upsert_watch_sessions_column_df_to_postgres(**context):
    data = context['ti']\
        .xcom_pull(key='movie_streaming_data', task_ids='generate_movie_streaming_data')
    df_watch_sessions = add_rundate(df=data['watch_sessions'], run_date_col='run_date_pg', logger=logger)
    df_to_postgres_table(
        df = df_watch_sessions,
        pg_table_name='watch_sessions',
        pg_schema_name='public',
        mode='upsert',
        conn_id='movie_db',
        logger=logger
    )

def upsert_subscriptions_column_df_to_postgres(**context):
    data = context['ti']\
        .xcom_pull(key='movie_streaming_data', task_ids='generate_movie_streaming_data')
    df_subscriptions = add_rundate(df=data['subscriptions'], run_date_col='run_date_pg', logger=logger)
    df_to_postgres_table(
        df = df_subscriptions,
        pg_table_name='subscriptions',
        pg_schema_name='public',
        mode='upsert',
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
    dag_id="dag_ingest_movie_streaming_to_postgres_hourly",
    description='Ingestion Movie Streaming hourly data to PostgreSQL (Create -> Insert)',
    tags=['hourly', 'ingestion', 'movie_streaming', 'postgres'],
    on_failure_callback=dag_failure_alert,
    on_success_callback=dag_success_alert,
    start_date=pendulum.datetime(2025, 9, 18, tz=local_tz),
    schedule_interval='@hourly',
    catchup=False
) as dag:

    generate_movie_streaming_data_task = PythonOperator(
        task_id="generate_movie_streaming_data",
        python_callable=generate_movie_streaming_data_hourly
    )

    upsert_users_column_df_to_postgres_task = PythonOperator(
        task_id="upsert_users_column_df_to_postgres",
        python_callable=upsert_users_column_df_to_postgres
    )

    upsert_movies_column_df_to_postgres_task = PythonOperator(
        task_id="upsert_movies_column_df_to_postgres",
        python_callable=upsert_movies_column_df_to_postgres
    )

    upsert_payments_column_df_to_postgres_task = PythonOperator(
        task_id="upsert_payments_column_df_to_postgres",
        python_callable=upsert_payments_column_df_to_postgres
    )

    upsert_ratings_column_df_to_postgres_task = PythonOperator(
        task_id="upsert_ratings_column_df_to_postgres",
        python_callable=upsert_ratings_column_df_to_postgres
    )

    upsert_watch_sessions_column_df_to_postgres_task = PythonOperator(
        task_id="upsert_watch_sessions_column_df_to_postgres",
        python_callable=upsert_watch_sessions_column_df_to_postgres
    )

    upsert_subscriptions_column_df_to_postgres_task = PythonOperator(
        task_id="upsert_subscriptions_column_df_to_postgres",
        python_callable=upsert_subscriptions_column_df_to_postgres
    )

    # users → parent for subscriptions, ratings, watch_sessions, payments
    # movies → parent for ratings, watch_sessions
    # subscriptions → parent for payments

    generate_movie_streaming_data_task >> [upsert_users_column_df_to_postgres_task,
                                       upsert_movies_column_df_to_postgres_task]

    upsert_users_column_df_to_postgres_task >> upsert_subscriptions_column_df_to_postgres_task

    upsert_subscriptions_column_df_to_postgres_task >> upsert_payments_column_df_to_postgres_task

    [upsert_users_column_df_to_postgres_task, upsert_movies_column_df_to_postgres_task] \
        >> upsert_ratings_column_df_to_postgres_task

    [upsert_users_column_df_to_postgres_task, upsert_movies_column_df_to_postgres_task] \
        >> upsert_watch_sessions_column_df_to_postgres_task

