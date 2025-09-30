import os
from dotenv import load_dotenv
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin
import pendulum
from airflow.operators.bash import BashOperator
from helpers.pubsub_helper import PubSubHelper
from helpers.bigquery_helper import BigQueryProject
from helpers.pubsub_helper import PubSubHelper

# ===============================================================================
load_dotenv("./env")
PROJECT_ID = os.getenv("BQ_PROJECT_ID")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

REGION = "us-central1"
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
TOPIC_ID = os.getenv("PUB_MOVIE_STREAMING_VIDEO_PLAYBACK_TOPIC")
SUBSCRIPTION_ID = os.getenv("SUB_VIDEO_PLAYBACK_DISCORD")


# ===============================================================================
logger = LoggingMixin().log
local_tz = pendulum.timezone("Asia/Jakarta")

# --- Instantiate project ---
project = BigQueryProject(
    project_id=PROJECT_ID,
    credentials_path=GOOGLE_APPLICATION_CREDENTIALS,
    location='us-central1',
    logger=logger
)

# ===============================================================================

def ensure_resources():
    # ensure subscription
    PubSubHelper(PROJECT_ID).create_subscription(TOPIC_ID, SUBSCRIPTION_ID)
# ===================================================================

with DAG(
    dag_id="dag_start_subscriber_alert_video_playback_events_discord",
    description="Start Dataflow streaming to send alert video playback events",
    tags=["movie_streaming", "subscriber", "dataflow", "pubsub", "discord"],
    start_date=pendulum.datetime(2025, 9, 24, tz=local_tz),
    schedule_interval=None,
    catchup=False,
) as dag:

    ensure_resources_task = PythonOperator(
        task_id="ensure_resources",
        python_callable=ensure_resources,
    )

    run_dataflow_pipeline = BashOperator(
        task_id="run_dataflow_pipeline",
        bash_command="python /opt/scripts/movie_streaming/subscriber_alert_discord_video_playback_events.py",
    )
    ensure_resources_task >> run_dataflow_pipeline