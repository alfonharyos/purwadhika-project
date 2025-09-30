from google.cloud import bigquery

VIDEO_PLAYBACK_EVENT_SCHEMA = [
    bigquery.SchemaField("event_id", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("session_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("user_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("movie_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("event_type", "STRING", mode="REQUIRED", description="start, buffer, error, resume, stop"),
    bigquery.SchemaField("event_timestamp", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("buffer_duration_ms", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("bitrate_kbps", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("city", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("isp", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("country", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("run_date", "DATE", mode="NULLABLE", description="Ingestion date (added by pipeline)"),
]