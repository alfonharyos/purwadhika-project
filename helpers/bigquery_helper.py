import json
import logging
import pandas as pd
from pathlib import Path
from typing import Optional, List, Dict
from datetime import datetime
from zoneinfo import ZoneInfo
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound
from google.cloud.bigquery import LoadJobConfig, TimePartitioning, WriteDisposition
import tempfile

# ---------------- PROJECT ----------------
class BigQueryProject:
    def __init__(
        self,
        project_id: str,
        credentials_path: str,
        location: str = "US",
        logger: Optional[logging.Logger] = None
    ):
        self.project_id = project_id
        self.credentials_path = credentials_path
        self.location = location

        if not self.project_id or not self.credentials_path:
            raise EnvironmentError("Missing BQ_PROJECT_ID or GOOGLE_APPLICATION_CREDENTIALS")

        creds = service_account.Credentials.from_service_account_file(self.credentials_path)
        self.client = bigquery.Client(credentials=creds, project=self.project_id, location=self.location)
        self.logger = logger or logging.getLogger(__name__)

    def dataset(self, dataset_id: str) -> "BigQueryDataset":
        """Return a dataset wrapper."""
        return BigQueryDataset(self, dataset_id)


# ---------- DATASET LEVEL ----------
class BigQueryDataset:
    def __init__(self, project: "BigQueryProject", dataset_id: str):
        self.project = project
        self.dataset_id = dataset_id
        self.client = project.client
        self.logger = project.logger

    def exists(self) -> bool:
        """Check if dataset exists in BigQuery."""
        try:
            self.client.get_dataset(f"{self.project.project_id}.{self.dataset_id}")
            self.logger.info(f"[BigQuery] Dataset {self.dataset_id} exists")
            return True
        except NotFound:
            self.logger.info(f"[BigQuery] Dataset {self.dataset_id} does not exist")
            return False

    def create(self, location: str = "US") -> None:
        """Create dataset if it does not exist."""
        dataset_ref = bigquery.Dataset(f"{self.project.project_id}.{self.dataset_id}")
        dataset_ref.location = location
        if not self.exists():
            try:
                self.client.create_dataset(dataset_ref)
                self.logger.info(f"[BigQuery] Created dataset {self.dataset_id} in {location}")
            except Exception as e:
                self.logger.error(f"[BigQuery] Failed to create dataset {self.dataset_id}: {e}")
                raise

    def table(self, table_id: str) -> "BigQueryTable":
        """Return a table wrapper."""
        return BigQueryTable(self, table_id)


# ---------- TABLE LEVEL ----------
class BigQueryTable:
    def __init__(self, dataset: BigQueryDataset, table_id: str):
        self.dataset = dataset
        self.table_id = table_id
        self.client = dataset.project.client
        self.logger = dataset.project.logger

    # ---------- SCHEMA ----------
    def load_schema_from_json(self, schema_path: str | Path) -> List[bigquery.SchemaField]:
        with open(schema_path, "r") as f:
            fields = json.load(f)
        return [
            bigquery.SchemaField(
                name=field["name"],
                field_type=field["type"],
                mode=field.get("mode", "NULLABLE"),
                description=field.get("description"),
            )
            for field in fields
        ]

    def get_json_schema(self, schema_path: str | Path) -> Dict[str, str]:
        with open(schema_path, "r") as f:
            schema = json.load(f)
        return {field["name"]: field["type"].upper() for field in schema}

    # ---------- CHECK TABLE ----------
    def exists(self) -> bool:
        try:
            self.client.get_table(f"{self.dataset.project.project_id}.{self.dataset.dataset_id}.{self.table_id}")
            return True
        except NotFound:
            return False

    # ---------- DELETE ---------- 
    def delete_where(self, condition: str):
        """
        Delete rows from table based on condition.
        Example:
            table.delete_where("run_date_bq = '2025-09-25'")
        """
        table_ref = f"{self.dataset.project.project_id}.{self.dataset.dataset_id}.{self.table_id}"
        query = f"DELETE FROM `{table_ref}` WHERE {condition}"
        job = self.client.query(query)
        result = job.result()  # wait for completion
        print(f"DELETE completed: {result.total_rows if hasattr(result, 'total_rows') else 'unknown'} rows affected")

    # ---------- TABLE CREATION ----------
    def create(
        self,
        schema: List[bigquery.SchemaField],
        partition_field: Optional[str] = None,
        clustering_fields: Optional[List[str]] = None,
        run_date_bq: bool = True,
    ):
        """
        Create a BigQuery table with optional partitioning, clustering, and run_date_bq.
        """
        table_ref = f"{self.dataset.project.project_id}.{self.dataset.dataset_id}.{self.table_id}"
        
        if run_date_bq and not any(f.name == "run_date_bq" for f in schema):
            schema.append(bigquery.SchemaField("run_date_bq", "DATE", mode="REQUIRED"))

        table = bigquery.Table(table_ref, schema=schema)
        
        # Partitioning
        if partition_field:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_field,
                require_partition_filter=False, 
            )

        if clustering_fields:
            table.clustering_fields = clustering_fields
        try:
            self.client.create_table(table, exists_ok=True)
            self.logger.info(f"[BigQuery] Created table {table_ref}")
        except Exception as e:
            self.logger.error(f"[BigQuery] Failed to create table {table_ref}: {e}")
            raise

    def full_table_id(self):
        return f"{self.dataset.project.project_id}.{self.dataset.dataset_id}.{self.table_id}"
    
    # ---------- INSERT TABLE ----------
    def insert_df(
        self,
        df: pd.DataFrame,
        schema: Optional[List[bigquery.SchemaField]] = None,
        partition_field: Optional[str] = None,
        if_exists: str = "append",
        run_date: str = 'run_date',
    ):
        """
        Insert DataFrame into BigQuery.
        Automatically adds 'run_date_bq' column if enabled.
        """
        # Auto-add run_date_bq
        if run_date:
            df[run_date] = datetime.now(ZoneInfo("Asia/Jakarta")).date()
            if schema is not None and not any(f.name == run_date for f in schema):
                schema.append(bigquery.SchemaField(run_date, "DATE", mode="REQUIRED"))

        # Config load job
        job_config_kwargs = dict(
            write_disposition=(
                WriteDisposition.WRITE_TRUNCATE
                if if_exists == "replace"
                else WriteDisposition.WRITE_APPEND
            ),
            schema=schema
        )
        if partition_field:
            job_config_kwargs["time_partitioning"] = TimePartitioning(
                field=partition_field, require_partition_filter=False
            )

        job_config = LoadJobConfig(**job_config_kwargs)

        full_table = self.full_table_id()
        try:
            job = self.client.load_table_from_dataframe(df, full_table, job_config=job_config)
            job.result()
            self.logger.info(f"[BigQuery] Inserted {len(df)} rows to {full_table} (mode={if_exists})")
        except Exception as e:
            self.logger.error(f"[BigQuery] Insert data to {full_table}: {e}")
            raise
    
    # Upload using temporary parquet
    def upload_with_temp_parquet(        
        self,
        df: pd.DataFrame,
        schema: Optional[List[bigquery.SchemaField]] = None,
        partition_field: Optional[str] = None,
        if_exists: str = "append"
    ):
        client = bigquery.Client()
        full_table = f"{self.dataset.project.project_id}.{self.dataset.dataset_id}.{self.table_id}"
        
        try:
            # Save DataFrame to temporary Parquet
            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                df.to_parquet(tmp.name, index=False)

                job_config = bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.PARQUET,
                    schema=schema,
                    write_disposition=(
                    bigquery.WriteDisposition.WRITE_TRUNCATE
                    if if_exists == "replace"
                    else bigquery.WriteDisposition.WRITE_APPEND
                    ),
                    time_partitioning=TimePartitioning(
                        field=partition_field, require_partition_filter=False
                    ),
                )

                with open(tmp.name, "rb") as f:
                    job = client.load_table_from_file(f, full_table, job_config=job_config)

            job.result()
            self.logger.info(f"[BigQuery] Loaded {job.output_rows} rows into {full_table}.")
        except Exception as e:
            self.logger.error(f"[BigQuery] Failed to Load {job.output_rows} rows into {full_table}: {e}")
            raise

                
    # Upload using temporary csv
    def upload_with_temp_csv(        
        self,
        df: pd.DataFrame,
        schema: Optional[List[bigquery.SchemaField]] = None,
        if_exists: str = "append"
    ):
        client = bigquery.Client()
        full_table = f"{self.dataset.project.project_id}.{self.dataset.dataset_id}.{self.table_id}"
        
        try:
            # Save DataFrame to temporary Parquet
            with tempfile.NamedTemporaryFile(suffix=".csv", mode="w", delete=False) as tmp:
                df.to_csv(tmp.name, index=False)
                tmp.flush()

                job_config = bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.CSV,
                    skip_leading_rows=1,
                    schema=schema,
                    write_disposition=(
                    bigquery.WriteDisposition.WRITE_TRUNCATE
                    if if_exists == "replace"
                    else bigquery.WriteDisposition.WRITE_APPEND
                    ),
                )

                with open(tmp.name, "rb") as f:
                    job = client.load_table_from_file(f, full_table, job_config=job_config)

            job.result()
            self.logger.info(f"[BigQuery] Loaded {job.output_rows} rows into {full_table}.")
        except Exception as e:
            self.logger.error(f"[BigQuery] Failed to Load {job.output_rows} rows into {full_table}: {e}")
            raise


    # ---------- MERGE ----------
    def merge_from(self, staging_table: "BigQueryTable", key_column: str):
        target = f"{self.dataset.project.project_id}.{self.dataset.dataset_id}.{self.table_id}"
        staging = f"{staging_table.dataset.project.project_id}.{staging_table.dataset.dataset_id}.{staging_table.table_id}"

        schema = self.client.get_table(target).schema
        columns = [f.name for f in schema]

        updates = ", ".join([f"T.{c}=S.{c}" for c in columns if c != key_column])
        cols_str = ", ".join(columns)
        vals_str = ", ".join([f"S.{c}" for c in columns])

        sql = f"""
        MERGE `{target}` T
        USING `{staging}` S
        ON T.{key_column} = S.{key_column}
        WHEN MATCHED THEN UPDATE SET {updates}
        WHEN NOT MATCHED THEN INSERT ({cols_str}) VALUES ({vals_str})
        """
        try:
            self.client.query(sql).result()
            self.logger.info(f"[BigQuery] Merged {staging} into {target} using key '{key_column}'")
        except Exception as e:
            self.logger.error(f"[BigQuery] Failed to merge {staging} into {target} using key '{key_column}: {e}")
            raise

    # ---------- EXTRACT ----------
    def extract_to_df(self, query: str) -> pd.DataFrame:
        try:
            df = self.client.query(query).to_dataframe()
            self.logger.info(f"[BigQuery] Extracted {len(df)} rows")
            return df
        except Exception as e:
            self.logger.error(f"[BigQuery] Failed to extract data: {e}")
            raise