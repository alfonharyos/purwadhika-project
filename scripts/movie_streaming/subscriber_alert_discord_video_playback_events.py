import os, json, apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import http.client
from urllib.parse import urlparse
from datetime import datetime
from zoneinfo import ZoneInfo
from airflow.utils.log.logging_mixin import LoggingMixin

logger = LoggingMixin().log



def parse_pubsub_message(message):
    payload = message.data if hasattr(message, "data") else message
    return json.loads(payload.decode("utf-8"))


def is_alert(record):
    return record.get("event_type") == "error" or int(record.get("buffer_duration_ms", 0)) > 4000


class DiscordBatchDoFn(beam.DoFn):
    def __init__(self, webhook_url):
        self.webhook_url = webhook_url

    def process(self, batch):
        if not self.webhook_url:
            return

        try:
            runtime = datetime.now(ZoneInfo("Asia/Jakarta")).strftime("%Y-%m-%d %H:%M:%S")
            # Gabungkan semua alert jadi satu pesan
            content = f"⚠️ {runtime} - {len(batch)} alerts\n"
            for r in batch:
                content += f"- User {r['user_id']} Movie {r['movie_id']} Event {r['event_type']} Buffer {r['buffer_duration_ms']}ms\n"

            parsed = urlparse(self.webhook_url)
            conn = http.client.HTTPSConnection(parsed.netloc, timeout=10)
            headers = {"Content-Type": "application/json"}
            body = json.dumps({"content": content})
            conn.request("POST", parsed.path, body=body, headers=headers)
            resp = conn.getresponse()
            logger.info(f"[Discord] status={resp.status}")
            conn.close()
        except Exception as e:
            logger.info(f"[Discord] Error sending batch: {e}")

        yield from batch  # supaya record tetap diteruskan kalau perlu


def run_alert_job():
    project_id = os.getenv("BQ_PROJECT_ID")
    subscription = f"projects/{project_id}/subscriptions/{os.getenv('SUB_VIDEO_PLAYBACK_DISCORD')}"
    gcs_bucket = os.getenv("GCS_BUCKET_NAME")
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL")

    with open(os.getenv("GOOGLE_APPLICATION_CREDENTIALS")) as f:
        sa_email = json.load(f)["client_email"]

    options = PipelineOptions([
        f"--project={project_id}",
        "--region=asia-southeast2",
        "--runner=DataflowRunner",
        "--streaming",
        f"--job_name=video-playback-to-discord",
        f"--temp_location=gs://{gcs_bucket}/movie_streaming/ataflow/tmp",
        f"--staging_location=gs://{gcs_bucket}/movie_streaming/dataflow/staging",
        f"--service_account_email={sa_email}",
        "--autoscaling_algorithm=THROUGHPUT_BASED",
        "--num_workers=2",
        "--max_num_workers=5",
    ])
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        alerts = (
            p
            | "Read" >> beam.io.ReadFromPubSub(subscription=subscription)
            | "Parse" >> beam.Map(parse_pubsub_message)
            | "FilterAlerts" >> beam.Filter(is_alert)
        )

        (
            alerts
            | "Window2Min" >> beam.WindowInto(beam.window.FixedWindows(120))
            | "BatchAlerts" >> beam.combiners.ToList().without_defaults()
            | "SendDiscordBatch" >> beam.ParDo(DiscordBatchDoFn(webhook_url))
        )


if __name__ == "__main__":
    run_alert_job()
