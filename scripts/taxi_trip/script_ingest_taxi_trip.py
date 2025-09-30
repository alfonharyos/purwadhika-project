import requests
import io
import os
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from helpers.dataframe_helper import normalize_column_names, rename_column_names

def fetch_taxi_data(year: int, month: int, taxi_type: str, logger) -> pa.Table | None:
    '''
    Downloads NYC yellow or green taxi trip data for the specified year and month.
    The data is in Parquet format and will be loaded using PyArrow.
    All column names are converted to lowercase for consistency.
    '''
    # Validate taxi_type
    if taxi_type not in {"yellow", "green"}:
        msg = f"Invalid taxi_type '{taxi_type}'. Must be 'yellow' or 'green'."
        logger.error(msg)
        raise ValueError(msg)
    
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month:02d}.parquet"
    logger.info(f"Fetching {taxi_type} taxi data for {year}-{month:02d} from {url}")
    
    try:
        # Download the data from the URL
        response = requests.get(url)
        response.raise_for_status()

        # Read the Parquet data into an in-memory buffer
        buffer = io.BytesIO(response.content)
        table = pq.read_table(buffer)

        # Convert all column names to lowercase
        lowercase_names = [col.lower() for col in table.schema.names]
        table = table.rename_columns(lowercase_names)

        logger.info(f"Parquet data fetched successfully with {table.num_rows:,} rows.")
        return table
    
    except Exception as e:
        logger.exception(f"Failed to fetch or parse data for {year}-{month:02d}: {e}")
        raise

def filter_weekend(table: pa.Table, taxi_type: str, logger) -> pd.DataFrame | None:
    '''
    Filters data to include only weekend (Pickup: Friday to Sunday)
    '''
    logger.info("Filtering for weekend night trips (Fri/Sat/Sun pickup after 6 PM, dropoff before midnight)")

    # Determine correct datetime columns
    if taxi_type == "yellow":
        pickup_col = "tpep_pickup_datetime"
        dropoff_col = "tpep_dropoff_datetime"
    elif taxi_type == "green":
        pickup_col = "lpep_pickup_datetime"
        dropoff_col = "lpep_dropoff_datetime"
    else:
        msg = f"Invalid taxi_type '{taxi_type}'. Must be 'yellow' or 'green'."
        logger.error(msg)
        raise ValueError(msg)

    # Validate filter columns
    for col in [pickup_col, dropoff_col]:
        if col not in table.schema.names:
            msg = f"Column {col} not found in the table."
            logger.error(msg)
            raise ValueError(msg)

    try:
        # Cast timestamps
        pickup_ts = pc.cast(table[pickup_col], pa.timestamp("s"))
        dropoff_ts = pc.cast(table[dropoff_col], pa.timestamp("s"))

        # Extract pickup filters
        pickup_day = pc.day_of_week(pickup_ts)
        pickup_hour = pc.hour(pickup_ts)
        is_weekend_pickup = pc.and_(
            pc.greater_equal(pickup_hour, pa.scalar(18)),
            pc.is_in(pickup_day, value_set=pa.array([4, 5, 6]))
        )

        # Extract drop-off filters
        dropoff_hour = pc.hour(dropoff_ts)
        is_valid_dropoff = pc.and_(
            pc.greater_equal(dropoff_hour, pa.scalar(18)),
            pc.less_equal(dropoff_hour, pa.scalar(23))
        )

        # Combine both filters
        is_valid_trip = pc.and_(is_weekend_pickup, is_valid_dropoff)
        filtered = table.filter(is_valid_trip)

        if filtered.num_rows == 0:
            logger.warning("No weekend night trips found with valid drop-off time.")
            return None

        logger.info(f"{filtered.num_rows:,} rows matching weekend night with drop-off filter extracted.")
        return filtered.to_pandas().drop_duplicates()

    except Exception as e:
        logger.exception(f"Filtering error: {e}")
        raise


def taxi_trip_weekend_monthly(year: int, month: int, taxi_type: str, logger) -> pd.DataFrame:
    try:
        logger.info(f"Starting {taxi_type} taxi data ingestion for {year}-{month:02d}")

        # Step 1: Fetch raw data
        taxi_data = fetch_taxi_data(year, month, taxi_type, logger)

        # Step 2: Filter for weekend trips
        taxi_data = filter_weekend(taxi_data, taxi_type, logger)

        # step 3: Normalize and Rename column names
        taxi_data = normalize_column_names(taxi_data, logger)
        rename_dict = {
                'tpep_pickup_datetime': 'pickup_datetime',
                'tpep_dropoff_datetime': 'dropoff_datetime',
                'lpep_pickup_datetime': 'pickup_datetime',
                'lpep_dropoff_datetime': 'dropoff_datetime'
            }
        taxi_data = rename_column_names(df=taxi_data, rename_dict=rename_dict, logger=logger)

        # Step 4: add taxi_type column
        taxi_data['taxi_type'] = taxi_type

        # setp 5: payment_type to integer
        taxi_data["payment_type"] = taxi_data["payment_type"].astype("Int64")

        logger.info(f"{taxi_type.capitalize()} taxi data ingestion for {year}-{month:02d} completed.") 
        return taxi_data
    except Exception as e:
        logger.error(f"Ingestion {taxi_type.capitalize()} FAILED: {e}")
        raise




# ===== TAXI ZONE =====================
def fetch_taxi_zone(logger):
    try:
        response = requests.get("https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv")
        response.raise_for_status()

        df = pd.read_csv(io.StringIO(response.text))
        df = normalize_column_names(df, logger)

        logger.info(f"Taxi zone fetched successfully")
        return df
    except Exception as e:
        logger.error(f"Error Fetch taxi zone data: {e}")
        raise