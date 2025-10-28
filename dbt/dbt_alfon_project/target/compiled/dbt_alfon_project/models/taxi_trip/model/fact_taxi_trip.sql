

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