-- back compat for old kwarg name
  
  
        
            
            
        
    

    

    merge into `purwadika`.`jcdeol004_alfon_taxi_trip_preparation`.`prep_taxi_trip` as DBT_INTERNAL_DEST
        using (

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
    
        where date(date_trunc(pickup_datetime, month)) >= (
            select date_sub(
                coalesce(date(max(date_trunc(pickup_datetime, month))), date('2023-01-01')),
                interval 2 month
            )
            from `purwadika`.`jcdeol004_alfon_taxi_trip_preparation`.`prep_taxi_trip`
        )
    
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
    
        where date(date_trunc(pickup_datetime, month)) >= (
            select date_sub(
                coalesce(date(max(date_trunc(pickup_datetime, month))), date('2023-01-01')),
                interval 2 month
            )
            from `purwadika`.`jcdeol004_alfon_taxi_trip_preparation`.`prep_taxi_trip`
        )
    
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
        ) as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.md5_key = DBT_INTERNAL_DEST.md5_key
            )

    
    when matched then update set
        `md5_key` = DBT_INTERNAL_SOURCE.`md5_key`,`taxi_type` = DBT_INTERNAL_SOURCE.`taxi_type`,`vendorid` = DBT_INTERNAL_SOURCE.`vendorid`,`pickup_datetime` = DBT_INTERNAL_SOURCE.`pickup_datetime`,`dropoff_datetime` = DBT_INTERNAL_SOURCE.`dropoff_datetime`,`pulocationid` = DBT_INTERNAL_SOURCE.`pulocationid`,`dolocationid` = DBT_INTERNAL_SOURCE.`dolocationid`,`payment_type` = DBT_INTERNAL_SOURCE.`payment_type`,`passenger_count` = DBT_INTERNAL_SOURCE.`passenger_count`,`fare_amount` = DBT_INTERNAL_SOURCE.`fare_amount`,`tip_amount` = DBT_INTERNAL_SOURCE.`tip_amount`,`total_amount` = DBT_INTERNAL_SOURCE.`total_amount`,`trip_distance` = DBT_INTERNAL_SOURCE.`trip_distance`,`run_date_bq` = DBT_INTERNAL_SOURCE.`run_date_bq`
    

    when not matched then insert
        (`md5_key`, `taxi_type`, `vendorid`, `pickup_datetime`, `dropoff_datetime`, `pulocationid`, `dolocationid`, `payment_type`, `passenger_count`, `fare_amount`, `tip_amount`, `total_amount`, `trip_distance`, `run_date_bq`)
    values
        (`md5_key`, `taxi_type`, `vendorid`, `pickup_datetime`, `dropoff_datetime`, `pulocationid`, `dolocationid`, `payment_type`, `passenger_count`, `fare_amount`, `tip_amount`, `total_amount`, `trip_distance`, `run_date_bq`)


    