
  
    

    create or replace table `purwadika`.`jcdeol004_alfon_taxi_trip_model`.`fact_taxi_trip`
      
    partition by timestamp_trunc(pickup_datetime, month)
    cluster by taxi_type, pulocationid

    OPTIONS()
    as (
      

with prep as (
    select *
    from `purwadika`.`jcdeol004_alfon_taxi_trip_preparation`.`prep_taxi_trip`
    
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
    );
  