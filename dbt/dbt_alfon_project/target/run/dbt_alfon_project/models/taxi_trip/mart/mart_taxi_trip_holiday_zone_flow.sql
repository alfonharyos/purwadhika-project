
  
    

    create or replace table `purwadika`.`jcdeol004_alfon_taxi_trip_mart`.`mart_taxi_trip_holiday_zone_flow`
      
    partition by trip_date
    cluster by pickup_zone, dropoff_zone

    OPTIONS()
    as (
      


with base as (
    select
        date(f.pickup_datetime) as trip_date,
        extract(hour from f.pickup_datetime) as trip_hour,
        l1.zone as pickup_zone,
        l2.zone as dropoff_zone,
        f.total_amount,
        f.fare_amount,
        f.tip_amount,
        f.trip_distance
    from `purwadika`.`jcdeol004_alfon_taxi_trip_model`.`fact_taxi_trip` f
    left join `purwadika`.`jcdeol004_alfon_taxi_trip_model`.`dim_location` l1
        on f.pulocationid = l1.locationid
    left join `purwadika`.`jcdeol004_alfon_taxi_trip_model`.`dim_location` l2
        on f.dolocationid = l2.locationid
    where exists (
        select 1
        from `purwadika`.`jcdeol004_alfon_taxi_trip_model`.`dim_holiday` h
        where h.date = date(f.pickup_datetime)
          and h.hour = extract(hour from f.pickup_datetime)
    )
)

select
    trip_date,
    trip_hour,
    pickup_zone,
    dropoff_zone,
    count(*) as total_trip,
    avg(total_amount) as avg_total_amount,
    sum(total_amount) as total_revenue,
    avg(trip_distance) as avg_distance,
    current_date("Asia/Jakarta") as run_date_bq
from base
group by 1,2,3,4
    );
  