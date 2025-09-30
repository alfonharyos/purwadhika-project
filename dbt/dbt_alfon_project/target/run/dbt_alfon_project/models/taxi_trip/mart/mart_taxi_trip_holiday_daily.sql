
  
    

    create or replace table `purwadika`.`jcdeol004_alfon_taxi_trip_mart`.`mart_taxi_trip_holiday_daily`
      
    
    

    OPTIONS()
    as (
      


select
    trip_date,
    sum(total_trip) as total_trip,
    sum(total_revenue) as total_revenue,
    sum(total_revenue) / sum(total_trip) as avg_total_amount,
    sum(avg_distance * total_trip) / sum(total_trip) as avg_distance,
    current_date("Asia/Jakarta") as run_date_bq
from `purwadika`.`jcdeol004_alfon_taxi_trip_mart`.`mart_taxi_trip_holiday_zone_flow`
group by trip_date
    );
  