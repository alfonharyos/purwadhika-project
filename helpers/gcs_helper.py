import os
import logging
import tempfile
import hashlib
import requests
from typing import Optional, List

import pandas as pd
from google.cloud import storage, bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound
from google.cloud.exceptions import GoogleCloudError


class GCSHelper:
    def __init__(
        self,
        bucket_name: str,
        project_id: str,
        credentials_path: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
    ):
        """
        Helper class for Google Cloud Storage and BigQuery interaction.
        """
        if not project_id or not credentials_path:
            raise EnvironmentError(
                "Missing one or more required environment variables: "
                "BQ_PROJECT_ID or GOOGLE_APPLICATION_CREDENTIALS."
            )

        self.project_id = project_id
        self.credentials = service_account.Credentials.from_service_account_file(credentials_path)

        # GCP clients
        self.gcs_client = storage.Client(project=project_id, credentials=self.credentials)
        self.bq_client = bigquery.Client(project=project_id, credentials=self.credentials)

        # Bucket reference
        self.bucket = self.gcs_client.bucket(bucket_name)

        # Logger
        self.logger = logger or logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

    # ------------------------
    # Basic GCS Operations
    # ------------------------
    def upload_file(self, local_path: str, gcs_path: str) -> None:
        """Upload file to GCS."""
        try:
            self.bucket.blob(gcs_path).upload_from_filename(local_path)
            self.logger.info(f"[GCS] Uploaded {local_path} → gs://{self.bucket.name}/{gcs_path}")
        except Exception as e:
            self.logger.error(f"[GCS] Failed to upload {local_path}: {e}", exc_info=True)
            raise


    def download_file(self, gcs_path: str, local_path: str) -> None:
        """Download file from GCS."""
        try:
            self.bucket.blob(gcs_path).download_to_filename(local_path)
            self.logger.info(f"[GCS] Downloaded gs://{self.bucket.name}/{gcs_path} → {local_path}")
        except Exception as e:
            self.logger.error(f"[GCS] Failed to download {gcs_path}: {e}", exc_info=True)
            raise

    def delete_file(self, gcs_path: str) -> None:
        """Delete file from GCS."""
        try:
            self.bucket.blob(gcs_path).delete()
            self.logger.info(f"[GCS] Deleted gs://{self.bucket.name}/{gcs_path}")
        except Exception as e:
            self.logger.error(f"[GCS] Failed to delete {gcs_path}: {e}", exc_info=True)
            raise

    def list_files(self, prefix: str = "") -> List[str]:
        """List objects in GCS with the given prefix."""
        try:
            blobs = self.gcs_client.list_blobs(self.bucket, prefix=prefix)
            files = [blob.name for blob in blobs]
            self.logger.info(f"[GCS] Found {len(files)} files under gs://{self.bucket.name}/{prefix}")
            return files
        except Exception as e:
            self.logger.error(f"[GCS] Failed to list files: {e}", exc_info=True)
            raise

    # ------------------------
    # DataFrame <-> GCS
    # ------------------------
    def df_to_gcs(
            self, 
            df: pd.DataFrame, 
            gcs_path: str, 
            file_format: str = "parquet",
            key_columns: list[str] = None,
            md5_column: Optional[str] = None,
    ) -> None:
        """Upload a pandas DataFrame to GCS as Parquet or CSV."""
        if file_format not in {"parquet", "csv"}:
            raise ValueError("file_format must be either 'parquet' or 'csv'")
        
        # --- optional md5 creation ---
        if key_columns:
            self.logger.info(f"[GCS] Generating md5 column '{md5_column}' using {key_columns}")
            def make_md5(row):
                key = "|".join([str(row[col]) if row[col] is not None else "" for col in key_columns])
                return hashlib.md5(key.encode("utf-8")).hexdigest()
            df[md5_column] = df.apply(make_md5, axis=1)

        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile(suffix=f".{file_format}", delete=False) as tmp_file:
                tmp_path = tmp_file.name

            if file_format == "parquet":
                df.to_parquet(tmp_path, index=False)
            else:
                df.to_csv(tmp_path, index=False)

            self.upload_file(tmp_path, gcs_path)
            self.logger.info(f"[GCS] Uploaded DataFrame → gs://{self.bucket.name}/{gcs_path} ({file_format.upper()})")
        except Exception as e:
            self.logger.error(f"[GCS] Failed to upload DataFrame: {e}", exc_info=True)
            raise
        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)

    # ------------------------
    # GCS -> BigQuery
    # ------------------------
    def gcs_to_bq(
        self,
        gcs_path: str,
        table_id: str,
        file_format: str = "parquet",
        write_disposition: str = "WRITE_APPEND",
        partition_field: Optional[str] = None,
        schema: Optional[List[bigquery.SchemaField]] = None,
        ignore_unknown_values: bool = True,
        autodetect: bool = False,
    ) -> None:
        """Load a file from GCS into BigQuery with optional schema and partitioning."""

        if file_format not in {"parquet", "csv"}:
            raise ValueError("file_format must be either 'parquet' or 'csv'")

        uri = f"gs://{self.bucket.name}/{gcs_path}"

        # Configure source format
        source_format = (
            bigquery.SourceFormat.PARQUET if file_format == "parquet" else bigquery.SourceFormat.CSV
        )

        # Build job configuration
        job_config = bigquery.LoadJobConfig(
            source_format=source_format,
            write_disposition=write_disposition,
            ignore_unknown_values=ignore_unknown_values,
            autodetect=autodetect,
            time_partitioning=bigquery.TimePartitioning(field=partition_field) if partition_field else None,
        )

        # Skip the header row
        if file_format == "csv":
            job_config.skip_leading_rows = 1

        # Override autodetect if schema is provided
        if schema:
            job_config.schema = schema
            job_config.autodetect = False

        try:
            load_job = self.bq_client.load_table_from_uri(uri, table_id, job_config=job_config)
            load_job.result()  # Wait for job to complete
            table = self.bq_client.get_table(table_id)
            self.logger.info(f"[BQ] Loaded {table.num_rows} rows into {table_id} from {uri}")
        except NotFound as e:
            self.logger.error(f"[BQ] Table or GCS file not found: {e}", exc_info=True)
            raise
        except GoogleCloudError as e:
            self.logger.error(f"[BQ] Google Cloud API error: {e}", exc_info=True)
            raise
        except Exception as e:
            self.logger.error(f"[BQ] Failed to load {uri} into {table_id}: {e}", exc_info=True)
            raise
