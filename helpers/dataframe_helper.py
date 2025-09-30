from datetime import datetime
from zoneinfo import ZoneInfo
import pandas as pd
import os
import hashlib

def load_csv_data(file_path: str, logger) -> pd.DataFrame:
    try:
        df = pd.read_csv(file_path, low_memory=False)
        logger.info(f"Data load successfully from {file_path} with {len(df)} rows.")
        return df
    except Exception as e:
        logger.error(f"Error loading CSV file at {file_path}: {e}")
        raise

def load_csv_data_by_month(file_path: str, year_col_name: str, month_col_name: str, year: int, month: int, logger) -> pd.DataFrame:
    try:
        df = load_csv_data(file_path=file_path, logger=logger)
        
        df = df[(df[f'{year_col_name}'] == year) &
                (df[f'{month_col_name}'] == month)]

        if df.empty:
            logger.warning(f"No data found for {month:02}-{year} from {file_path}")
        else:
            logger.info(f"Data load successfully for {month:02}-{year} from {file_path} with {len(df)} rows")

        return df
    except Exception as e:
        logger.error(f"Error loading mart data from {file_path}: {e}")
        raise

def validate_required_columns(df: pd.DataFrame, required_columns: list[str], logger) -> None:
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        logger.error(f"Missing required columns: {missing_cols}")
        raise
    logger.info("All required columns are present.")

def select_required_columns(df: pd.DataFrame, required_columns: list[str], logger) -> pd.DataFrame:
    try:
        validate_required_columns(df, required_columns, logger)
        df = df[required_columns]
        logger.info(f"Selected required columns: {required_columns}")
        return df
    except Exception as e:
        logger.error(f"Error selecting required columns: {e}")
        raise

def normalize_column_names(df: pd.DataFrame, logger) -> pd.DataFrame:
    original_columns = df.columns.tolist()
    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]
    logger.info(f"Normalized columns from {original_columns} to {df.columns.tolist()}")
    return df

def rename_column_names(df: pd.DataFrame, rename_dict: dict, logger) -> pd.DataFrame:
    original_columns = df.columns.tolist()
    df = df.rename(columns=rename_dict)
    updated_columns = df.columns.tolist()
    logger.info(f"Original columns: {original_columns}")
    logger.info(f"Updated columns: {updated_columns}")
    return df 

def datetime_expand(df: pd.DataFrame, datetime_col: str, logger) -> pd.DataFrame:
    if datetime_col not in df.columns:
        logger.error(f"Column '{datetime_col}' not found in DataFrame.")
        raise
    try:
        df[datetime_col] = pd.to_datetime(df[datetime_col], errors='coerce')
        if df[datetime_col].isnull().all():
            logger.error(f"All values in column '{datetime_col}' could not be converted to datetime.")
            raise
        
        df[f'{datetime_col}_year'] = df[datetime_col].dt.year
        df[f'{datetime_col}_month'] = df[datetime_col].dt.month
        df[f'{datetime_col}_day'] = df[datetime_col].dt.day
        df[f'{datetime_col}_hour'] = df[datetime_col].dt.hour
        df[f'{datetime_col}_day_of_week'] = df[datetime_col].dt.dayofweek
        
        logger.info(f"Expanded datetime column '{datetime_col}' into separate components.")
        return df
    except Exception as e:
        logger.error(f"Error processing datetime column '{datetime_col}': {e}")
        raise

def add_rundate(df: pd.DataFrame, run_date_col: str, logger) -> pd.DataFrame:
    # Add rundate column in Jakarta GMT+7
    df[f"{run_date_col}"] = datetime.now(ZoneInfo("Asia/Jakarta")).date()
    logger.info(f"'run_date_GMT+7' column added to the DataFrame with {len(df)} rows.")
    return df

def generate_unique_id(df: pd.DataFrame, cols_to_hash: list, unique_id_column_name: str, logger) -> pd.DataFrame:
    try:
        logger.info(f"Generating {unique_id_column_name}...")

        # Validate required columns
        missing_cols = [col for col in cols_to_hash if col not in df.columns]
        if missing_cols:
            raise KeyError(f"Missing columns for hashing: {missing_cols}")

        # Generate SHA256 hash for each row
        def hash_row(row):
            raw_string = '-'.join(str(row[col]) for col in cols_to_hash)
            return hashlib.sha256(raw_string.encode('utf-8')).hexdigest()

        df[unique_id_column_name] = df.apply(hash_row, axis=1)

        # Reorder columns to place unique_id first
        cols = [unique_id_column_name] + [col for col in df.columns if col != unique_id_column_name]
        df = df[cols]

        logger.info(f"Successfully generated {unique_id_column_name} and moved it to the first column.")
        return df

    except Exception as e:
        logger.error(f"Error generating {unique_id_column_name}: {e}")
        raise

def upsert_df_to_file(
    df: pd.DataFrame,
    target_file_path: str, 
    unique_id: list, 
    keep: str = "last", 
    logger = None
) -> None: 
    if os.path.exists(target_file_path): 
        logger.info(f"target file {target_file_path} exists. Merging data...")
        try: 
            target_file_df = pd.read_csv(target_file_path) 
            combined_df = pd.concat([target_file_df, df]).drop_duplicates(subset=unique_id, keep=keep) 
            combined_df.to_csv(target_file_path, index=False) 
            logger.info(f"Merged data saved to {target_file_path} with {len(combined_df)} total rows.") 
        except Exception as e:
            logger.error(f"Error merging data to {target_file_path}: {e}")
            raise
    else: 
        logger.info(f"target file {target_file_path} does not exist. Creating new file...") 
        os.makedirs(os.path.dirname(target_file_path), exist_ok=True) 
        df.to_csv(target_file_path, index=False) 
        logger.info(f"New target file created at {target_file_path} with {len(df)} rows.")

def cast_types_for_bq(df: pd.DataFrame, schema) -> pd.DataFrame:
    for field in schema:
        col = field.name
        if col not in df.columns:
            continue

        if field.field_type == "DATE":
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
        elif field.field_type == "INT64":
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df