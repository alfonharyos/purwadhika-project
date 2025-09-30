import os, json, apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

def parse_pubsub_message(message):
    payload = message.data if hasattr(message, "data") else message
    return json.loads(payload.decode("utf-8"))

def is_alert(record):
    return record.get("event_type") == "error" or int(record.get("buffer_duration_ms", 0)) > 4000

def run_bigquery_job():
    project_id = os.getenv("BQ_PROJECT_ID")
    subscription = f"projects/{project_id}/subscriptions/{os.getenv('SUB_VIDEO_PLAYBACK_BIGQUERY')}"
    bq_table = f"{project_id}:{os.getenv('SUB_DATASET_ID')}.{os.getenv('SUB_TABLE_ID')}"
    gcs_bucket = os.getenv("GCS_BUCKET_NAME")

    with open(os.getenv("GOOGLE_APPLICATION_CREDENTIALS")) as f: 
        sa_email = json.load(f)["client_email"]

    options = PipelineOptions([
        f"--project={project_id}",
        "--region=asia-southeast2",
        "--runner=DataflowRunner",
        "--streaming",
        f"--job_name=video-playback-to-bq",
        f"--temp_location=gs://{gcs_bucket}/movie_streaming/dataflow/tmp",
        f"--staging_location=gs://{gcs_bucket}/movie_streaming/dataflow/staging",
        f"--service_account_email={sa_email}",
    ])
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        (
            p
            | "Read" >> beam.io.ReadFromPubSub(subscription=subscription)
            | "Parse" >> beam.Map(parse_pubsub_message)
            | "FilterAlerts" >> beam.Filter(is_alert)
            | "WriteToBQ" >> beam.io.WriteToBigQuery(
                table=bq_table,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                method="STREAMING_INSERTS",
            )
        )

if __name__ == "__main__":
    run_bigquery_job()
