-- back compat for old kwarg name
  
  
        
            
            
        
    

    

    merge into `purwadika`.`jcdeol004_alfon_taxi_trip_model`.`fact_taxi_trip` as DBT_INTERNAL_DEST
        using (

with prep as (
    select *
    from `purwadika`.`jcdeol004_alfon_taxi_trip_preparation`.`prep_taxi_trip`
    
        where date(date_trunc(pickup_datetime, month)) >= (
            select date_sub(
                coalesce(date(max(date_trunc(pickup_datetime, month))), date('2023-01-01')),
                interval 2 month
            )
            from `purwadika`.`jcdeol004_alfon_taxi_trip_model`.`fact_taxi_trip`
        )
    
)

select
    md5_key,
    taxi_type,
    pickup_datetime,
    dropoff_datetime,
    pulocationid,
    dolocationid,
    passenger_count,
    cast(payment_type as int64) as payment_type,
    fare_amount,
    tip_amount,
    total_amount,
    trip_distance,
    current_date("Asia/Jakarta") as run_date_bq
from prep
        ) as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.md5_key = DBT_INTERNAL_DEST.md5_key
            )

    
    when matched then update set
        `md5_key` = DBT_INTERNAL_SOURCE.`md5_key`,`taxi_type` = DBT_INTERNAL_SOURCE.`taxi_type`,`pickup_datetime` = DBT_INTERNAL_SOURCE.`pickup_datetime`,`dropoff_datetime` = DBT_INTERNAL_SOURCE.`dropoff_datetime`,`pulocationid` = DBT_INTERNAL_SOURCE.`pulocationid`,`dolocationid` = DBT_INTERNAL_SOURCE.`dolocationid`,`passenger_count` = DBT_INTERNAL_SOURCE.`passenger_count`,`payment_type` = DBT_INTERNAL_SOURCE.`payment_type`,`fare_amount` = DBT_INTERNAL_SOURCE.`fare_amount`,`tip_amount` = DBT_INTERNAL_SOURCE.`tip_amount`,`total_amount` = DBT_INTERNAL_SOURCE.`total_amount`,`trip_distance` = DBT_INTERNAL_SOURCE.`trip_distance`,`run_date_bq` = DBT_INTERNAL_SOURCE.`run_date_bq`
    

    when not matched then insert
        (`md5_key`, `taxi_type`, `pickup_datetime`, `dropoff_datetime`, `pulocationid`, `dolocationid`, `passenger_count`, `payment_type`, `fare_amount`, `tip_amount`, `total_amount`, `trip_distance`, `run_date_bq`)
    values
        (`md5_key`, `taxi_type`, `pickup_datetime`, `dropoff_datetime`, `pulocationid`, `dolocationid`, `passenger_count`, `payment_type`, `fare_amount`, `tip_amount`, `total_amount`, `trip_distance`, `run_date_bq`)


    