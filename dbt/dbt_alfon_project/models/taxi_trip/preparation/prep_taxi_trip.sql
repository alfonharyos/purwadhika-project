{{ config(
    materialized='incremental',
    incremental_strategy='merge', 
    unique_key='md5_key',
    partition_by={"field": "pickup_datetime", "data_type": "timestamp", "granularity": "month"},
    cluster_by=["taxi_type", "pulocationid"],
    tags=['preparation']
) }}

-- ----------------------------------
-- Step 1: Filter by month (incremental)
-- ----------------------------------
with yellow_filtered as (
    select 
        md5_key,
        taxi_type,
        vendorid,
        pickup_datetime,
        dropoff_datetime,
        pulocationid,
        dolocationid,
        cast(payment_type as int64) as payment_type,
        passenger_count,
        fare_amount,
        tip_amount,
        total_amount,
        trip_distance
    from {{ source('taxi_trip_raw', 'raw_yellow_taxi_trip') }}
    {% if is_incremental() %}
        where date(date_trunc(pickup_datetime, month)) >= (
            select date_sub(
                coalesce(date(max(date_trunc(pickup_datetime, month))), date('2023-01-01')),
                interval 2 month
            )
            from {{ this }}
        )
    {% endif %}
),

green_filtered as (
    select
        md5_key,
        taxi_type,
        vendorid,
        pickup_datetime,
        dropoff_datetime,
        pulocationid,
        dolocationid,
        cast(payment_type as int64) as payment_type,
        passenger_count,
        fare_amount,
        tip_amount,
        total_amount,
        trip_distance
    from {{ source('taxi_trip_raw', 'raw_green_taxi_trip') }}
    {% if is_incremental() %}
        where date(date_trunc(pickup_datetime, month)) >= (
            select date_sub(
                coalesce(date(max(date_trunc(pickup_datetime, month))), date('2023-01-01')),
                interval 2 month
            )
            from {{ this }}
        )
    {% endif %}
),

-- ----------------------------------
-- Step 2: Union filtered data
-- ----------------------------------
unioned as (
    select * from yellow_filtered
    union all
    select * from green_filtered
),

-- ----------------------------------
-- Step 3: Clean invalid rows
-- ----------------------------------
cleaned as (
    select
        md5_key,
        taxi_type,
        vendorid,
        pickup_datetime,
        dropoff_datetime,
        pulocationid,
        dolocationid,
        payment_type,
        passenger_count,
        fare_amount,
        tip_amount,
        total_amount,
        trip_distance
    from unioned
    where trip_distance > 0
      and fare_amount > 0
      and total_amount > 0
      and passenger_count > 0
      and pickup_datetime < dropoff_datetime
),

-- ----------------------------------
-- Step 4: Deduplicate by md5_key (QUALIFY)
-- ----------------------------------
deduped as (
    select
        md5_key,
        taxi_type,
        vendorid,
        pickup_datetime,
        dropoff_datetime,
        pulocationid,
        dolocationid,
        payment_type,
        passenger_count,
        fare_amount,
        tip_amount,
        total_amount,
        trip_distance
    from cleaned
    qualify row_number() over (
        partition by md5_key
        order by pickup_datetime asc
    ) = 1
)

-- ----------------------------------
-- Final select
-- ----------------------------------
select
    md5_key,
    taxi_type,
    vendorid,
    pickup_datetime,
    dropoff_datetime,
    pulocationid,
    dolocationid,
    payment_type,
    passenger_count,
    fare_amount,
    tip_amount,
    total_amount,
    trip_distance,
    current_date("Asia/Jakarta") as run_date_bq
from deduped
