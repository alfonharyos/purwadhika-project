import pandas as pd
from typing import Optional, Union, List, Dict
import decimal
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import sql
from psycopg2.extras import execute_values
from google.cloud import bigquery
from helpers.dataframe_helper import validate_required_columns

# =============================================================================
# GENERIC QUERY EXECUTION
# =============================================================================
def execute_query(query: str, conn_id: str = "postgres_default", logger=None):
    """
    Execute non-SELECT query using Airflow PostgresHook.
    """
    try:
        hook = PostgresHook(postgres_conn_id=conn_id)
        hook.run(query)
        if logger:
            logger.info("Query executed successfully.")
    except Exception as e:
        if logger:
            logger.error(f"Failed to execute query: {e}\nQuery: {query}")
        raise

def fetch_query(query: str, conn_id: str = "postgres_default", logger=None):
    """
    Execute SELECT query and return rows.
    """
    try:
        hook = PostgresHook(postgres_conn_id=conn_id)
        rows = hook.get_records(sql=query)
        if logger:
            logger.info(f"Fetched {len(rows)} rows.")
        return rows
    except Exception as e:
        if logger:
            logger.error(f"Failed to fetch query: {e}\nQuery: {query}")
        raise

# =============================================================================
# TABLE UTILITIES
# =============================================================================
def get_last_id(table_name: str, id_column: str, conn_id: str = "movie_db", logger=None) -> int:
    try:
        hook = PostgresHook(postgres_conn_id=conn_id)
        sql = f"SELECT COALESCE(MAX({id_column}), 0) FROM {table_name};"
        record = hook.get_first(sql)
        last_id = record[0] if record and record[0] is not None else 0
        return int(last_id)
    except Exception as e:
        if logger:
            logger.error(f"Failed to get last id from {table_name}.{id_column}: {e}")
        raise

def get_table_names(schema: str = "public", conn_id: str = "postgres_default", logger=None) -> list:
    """
    Get all table names in a schema.
    """
    sql_query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = %s;
    """
    try:
        hook = PostgresHook(postgres_conn_id=conn_id)
        rows = hook.get_records(sql=sql_query, parameters=(schema,))
        tables = [row[0] for row in rows]
        if logger:
            logger.info(f"Retrieved tables: {tables}")
        return tables
    except Exception as e:
        if logger:
            logger.error(f"Failed to get table names: {e}")
        raise

def get_table_column_info(
    table_name: str,
    schema_name: str = "public",
    conn_id: str = "postgres_default",
    logger=None
) -> Dict[str, List[str]]:
    """
    Get column names, primary keys, and unique keys for a given table in PostgreSQL.
    Prioritize PK as unique_column_names, fallback to UNIQUE if no PK exists.

    Returns:
        {
            "column_names": [...],
            "unique_column_names": [...]  # PK if exists, else UNIQUE
        }
    """
    hook = PostgresHook(postgres_conn_id=conn_id)

    try:
        # 1. Get all columns
        col_query = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
            ORDER BY ordinal_position;
        """
        col_rows = hook.get_records(sql=col_query, parameters=(schema_name, table_name))
        column_names = [r[0] for r in col_rows]

        # 2. Get primary keys
        pk_query = """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_schema = %s
              AND tc.table_name = %s;
        """
        pk_rows = hook.get_records(sql=pk_query, parameters=(schema_name, table_name))
        pk_columns = [r[0] for r in pk_rows]

        # 3. Get unique (non-PK) columns
        unique_query = """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'UNIQUE'
              AND tc.table_schema = %s
              AND tc.table_name = %s;
        """
        unique_rows = hook.get_records(sql=unique_query, parameters=(schema_name, table_name))
        unique_columns = [r[0] for r in unique_rows]

        # 4. Prioritize PK if exists, else UNIQUE
        if pk_columns:
            conflict_columns = pk_columns
        else:
            conflict_columns = unique_columns

        if logger:
            logger.info(
                f"Table {schema_name}.{table_name} → Columns: {column_names}, "
                f"PK: {pk_columns}, UNIQUE: {unique_columns}"
            )

        return {
            "column_names": column_names,
            "unique_column_names": conflict_columns
        }

    except Exception as e:
        if logger:
            logger.error(f"Error retrieving column info for {schema_name}.{table_name}: {e}")
        raise


# =============================================================================
# EXECUTE SQL FROM FILE
# =============================================================================
def execute_sql_from_file(filepath: str, conn_id: str = "postgres_default", logger=None):
    """Execute SQL file using Airflow PostgresHook."""
    try:
        with open(filepath, "r") as f:
            sql_content = f.read()
        execute_query(sql_content, conn_id=conn_id, logger=logger)
        if logger:
            logger.info(f"Executed SQL file: {filepath}")
    except Exception as e:
        if logger:
            logger.error(f"Error executing SQL file {filepath}: {e}")
        raise

# =============================================================================
# BULK INSERT / UPSERT FROM DATAFRAME
# =============================================================================

def build_insert_sql(schema, table, columns, mode, unique_cols, conn):
    col_identifiers = sql.SQL(", ").join(map(sql.Identifier, columns))
    table_identifier = sql.Identifier(schema, table)

    if mode == "insert":
        return sql.SQL("INSERT INTO {} ({}) VALUES %s").format(table_identifier, col_identifiers).as_string(conn)

    conflict_clause = sql.SQL(", ").join(map(sql.Identifier, unique_cols))

    if mode == "insert_ignore":
        return sql.SQL("""
            INSERT INTO {} ({})
            VALUES %s
            ON CONFLICT ({}) DO NOTHING
        """).format(table_identifier, col_identifiers, conflict_clause).as_string(conn)

    if mode == "upsert":
        set_clause = sql.SQL(", ").join(
            sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
            for c in columns if c not in unique_cols
        )
        return sql.SQL("""
            INSERT INTO {} ({})
            VALUES %s
            ON CONFLICT ({}) DO UPDATE SET {}
        """).format(table_identifier, col_identifiers, conflict_clause, set_clause).as_string(conn)

def df_to_postgres_table(
    df: pd.DataFrame,
    pg_table_name: str,
    pg_schema_name: str = "public",
    mode: str = "insert",  # "insert", "upsert", "insert_ignore"
    conn_id: str = "postgres_default",
    commit_every: int = 10000,
    logger=None
):
    """
    Insert atau upsert DataFrame ke PostgreSQL via Airflow PostgresHook.
    mode:
      - "insert"         : insert biasa (akan error jika PK duplicate)
      - "insert_ignore"  : insert dengan ON CONFLICT DO NOTHING
      - "upsert"         : insert/update jika PK duplicate
    """
        
    if df.empty:
        if logger:
            logger.warning(f"Skipped empty DataFrame for table '{pg_table_name}'.")
        return

    if mode not in ("insert", "upsert", "insert_ignore"):
        raise ValueError("mode must be 'insert', 'upsert', or 'insert_ignore'")

    # Metadata tabel
    table_info = get_table_column_info(
        table_name=pg_table_name,
        schema_name=pg_schema_name,
        conn_id=conn_id,
        logger=logger
    )
    column_names = table_info['column_names']
    unique_columns = table_info['unique_column_names']

    validate_required_columns(df=df, required_columns=column_names, logger=logger)

    # Normalisasi unique_columns
    unique_cols = []
    if mode in ("upsert", "insert_ignore"):
        if not unique_columns:
            raise ValueError(f"unique_columns is required for mode '{mode}'")
        unique_cols = [unique_columns] if isinstance(unique_columns, str) else list(unique_columns)

    cols = list(df.columns)
    values = df.values.tolist()
    hook = PostgresHook(postgres_conn_id=conn_id)

    try:
        conn = hook.get_conn()
        with conn.cursor() as cur:
            insert_sql = build_insert_sql(
                schema=pg_schema_name,
                table=pg_table_name,
                columns=cols,
                mode=mode,
                unique_cols=unique_cols,
                conn=conn
            )

            # Eksekusi batch
            for i in range(0, len(values), commit_every):
                batch = values[i:i+commit_every]
                execute_values(cur, insert_sql, batch)
                conn.commit()
                if logger:
                    logger.info(f"Committed batch {i//commit_every + 1} ({len(batch)} rows) into '{pg_table_name}'.")

        if logger:
            logger.info(f"{mode.capitalize()} completed: {len(values):,} rows into '{pg_table_name}'.")

    except Exception as e:
        if logger:
            logger.error(f"Failed to {mode} data into '{pg_table_name}': {e}")
        raise
    

# =============================================================================
# EXTRACT TO DATAFRAME
# =============================================================================
def extract_postgres_to_df(query: str, conn_id: str = "postgres_default", logger=None) -> pd.DataFrame:
    """Extract query result to Pandas DataFrame using Airflow PostgresHook."""
    try:
        hook = PostgresHook(postgres_conn_id=conn_id)
        df = hook.get_pandas_df(sql=query)
        if logger:
            logger.info(f"Extracted {len(df)} rows.")
        return df
    except Exception as e:
        if logger:
            logger.error(f"Error extracting data: {e}")
        raise

# =============================================================================
# POSTGRES SCHEMA
# =============================================================================
def map_pg_to_bq(pg_type: str) -> str:
    pg_type = pg_type.lower()
    if "char" in pg_type or "text" in pg_type:
        return "STRING"
    if "int" in pg_type or pg_type in ("serial", "bigserial"):
        return "INTEGER"
    if "numeric" in pg_type or "decimal" in pg_type:
        return "NUMERIC"
    if "double" in pg_type or "real" in pg_type:
        return "FLOAT"
    if "bool" in pg_type:
        return "BOOL"
    if pg_type == "date":
        return "DATE"
    if "time" == pg_type:
        return "TIME"
    if "timestamp" in pg_type:
        return "TIMESTAMP"
    return "STRING"

def get_postgres_to_bq_schema(table_name: str, conn_id: str = "movie_db", logger=None) -> list[bigquery.SchemaField]:
    try:
        hook = PostgresHook(postgres_conn_id=conn_id)
        sql = f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position
        """
        rows = hook.get_records(sql)

        pk_sql = f"""
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_name = kcu.table_name
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_name = '{table_name}'
        """
        pk_rows = hook.get_records(pk_sql)
        pk_columns = {r[0] for r in pk_rows}

        schema = []
        for col_name, pg_type in rows:
            bq_type = map_pg_to_bq(pg_type)
            mode = "REQUIRED" if col_name in pk_columns else "NULLABLE"
            schema.append(bigquery.SchemaField(col_name, bq_type, mode=mode))
            logger.info(f"[get_postgres_to_bq_schema] Column '{col_name}': pg {pg_type} → bq {bq_type}")
        return schema
    except Exception as e:
        if logger:
            logger.error(f"Error extracting postgres schema: {e}")
        raise


# =============================================================================
# PREPARE DATAFRAME TO BIGQUERY
# =============================================================================
def align_df_to_bq_schema(df: pd.DataFrame, schema: list[bigquery.SchemaField], logger) -> pd.DataFrame:
    """
    Align DataFrame columns to BigQuery schema types.
    Converts NUMERIC/DECIMAL fields to Decimal, INT64 to int, FLOAT64 to float, etc.
    """

    df = df.copy()

    for field in schema:
        col = field.name
        if col not in df.columns:
            logger.info(f"[align_df_to_bq_schema] Column {col} not found in DataFrame, skipping")
            continue

        bq_type = field.field_type.upper()

        try:
            if bq_type in ("INT64", "INTEGER"):
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
                logger.info(f"[align_df_to_bq_schema] Converted {col} → INT64")

            elif bq_type in ("FLOAT64", "FLOAT"):
                df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)
                logger.info(f"[align_df_to_bq_schema] Converted {col} → FLOAT64")

            elif bq_type in ("NUMERIC", "DECIMAL", "BIGNUMERIC", "BIGDECIMAL"):
                # BigQuery requires Decimal or string for NUMERIC
                scale = 2  # default, can parse from field.description if needed
                df[col] = df[col].apply(
                    lambda x: decimal.Decimal(str(round(float(x), scale)))
                    if pd.notnull(x) else None
                )
                logger.info(f"[align_df_to_bq_schema] Converted {col} → NUMERIC(Decimal with {scale} digits)")

            elif bq_type in ("BOOL", "BOOLEAN"):
                df[col] = df[col].astype(bool)
                logger.info(f"[align_df_to_bq_schema] Converted {col} → BOOLEAN")

            elif bq_type in ("DATE",):
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
                logger.info(f"[align_df_to_bq_schema] Converted {col} → DATE")

            elif bq_type in ("DATETIME", "TIMESTAMP"):
                df[col] = pd.to_datetime(df[col], errors="coerce")
                logger.info(f"[align_df_to_bq_schema] Converted {col} → {bq_type}")

            else:
                # Default: cast everything else to str
                df[col] = df[col].astype(str)
                logger.info(f"[align_df_to_bq_schema] Converted {col} → STRING (fallback)")

        except Exception as e:
            logger.error(f"[align_df_to_bq_schema] Failed converting {col} to {bq_type}: {e}")

    return df