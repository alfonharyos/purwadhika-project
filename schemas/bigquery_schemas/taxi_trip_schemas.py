from google.cloud import bigquery

YELLOW_TAXI_SCHEMA = [
    bigquery.SchemaField("taxi_type", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("vendorid", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("pickup_datetime", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("dropoff_datetime", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("trip_distance", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("pulocationid", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("dolocationid", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("payment_type", "INTEGER"),
    bigquery.SchemaField("total_amount", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("passenger_count", "FLOAT"),
    bigquery.SchemaField("ratecodeid", "FLOAT"),
    bigquery.SchemaField("store_and_fwd_flag", "STRING"),
    bigquery.SchemaField("fare_amount", "FLOAT"),
    bigquery.SchemaField("extra", "FLOAT"),
    bigquery.SchemaField("mta_tax", "FLOAT"),
    bigquery.SchemaField("tip_amount", "FLOAT"),
    bigquery.SchemaField("tolls_amount", "FLOAT"),
    bigquery.SchemaField("improvement_surcharge", "FLOAT"),
    bigquery.SchemaField("congestion_surcharge", "FLOAT"),
    bigquery.SchemaField("airport_fee", "FLOAT"),
    bigquery.SchemaField("cbd_congestion_fee", "FLOAT"),
    bigquery.SchemaField("md5_key", "STRING"),
    bigquery.SchemaField("run_date_bq", "DATE", mode="REQUIRED")
]

GREEN_TAXI_SCHEMA = [
    bigquery.SchemaField("taxi_type", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("vendorid", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("pickup_datetime", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("dropoff_datetime", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("trip_distance", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("pulocationid", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("dolocationid", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("payment_type", "INTEGER"),
    bigquery.SchemaField("total_amount", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("passenger_count", "FLOAT"),
    bigquery.SchemaField("ratecodeid", "FLOAT"),
    bigquery.SchemaField("store_and_fwd_flag", "STRING"),
    bigquery.SchemaField("fare_amount", "FLOAT"),
    bigquery.SchemaField("extra", "FLOAT"),
    bigquery.SchemaField("mta_tax", "FLOAT"),
    bigquery.SchemaField("tip_amount", "FLOAT"),
    bigquery.SchemaField("tolls_amount", "FLOAT"),
    bigquery.SchemaField("improvement_surcharge", "FLOAT"),
    bigquery.SchemaField("congestion_surcharge", "FLOAT"),
    bigquery.SchemaField("cbd_congestion_fee", "FLOAT"),
    bigquery.SchemaField("md5_key", "STRING"),
    bigquery.SchemaField("run_date_bq", "DATE", mode="REQUIRED")
]


TAXI_ZONE_SCHEMA = [
    bigquery.SchemaField("locationid", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("borough", "STRING"),
    bigquery.SchemaField("zone", "STRING"),
    bigquery.SchemaField("service_zone", "STRING"),
    bigquery.SchemaField("run_date_bq", "DATE", mode="REQUIRED")
]