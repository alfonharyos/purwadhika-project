

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
    from `purwadika`.`jcdeol004_alfon_taxi_trip_raw`.`raw_yellow_taxi_trip`
    
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
    from `purwadika`.`jcdeol004_alfon_taxi_trip_raw`.`raw_green_taxi_trip`
    
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