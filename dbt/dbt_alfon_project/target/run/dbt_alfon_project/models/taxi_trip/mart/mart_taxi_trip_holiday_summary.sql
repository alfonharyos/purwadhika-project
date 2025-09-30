
  
    

    create or replace table `purwadika`.`jcdeol004_alfon_taxi_trip_mart`.`mart_taxi_trip_holiday_summary`
      
    
    

    OPTIONS()
    as (
      


with daily as (
    select *
    from `purwadika`.`jcdeol004_alfon_taxi_trip_mart`.`mart_taxi_trip_holiday_daily`
),
holiday_type as (
    select date, holiday_type
    from `purwadika`.`jcdeol004_alfon_taxi_trip_model`.`dim_holiday`
)

select
    extract(year from d.trip_date) as year,
    extract(month from d.trip_date) as month,
    h.holiday_type,
    sum(d.total_trip) as total_trip,
    sum(d.total_revenue) as total_revenue,
    sum(d.total_revenue) / sum(d.total_trip) as avg_total_amount,
    sum(d.avg_distance * d.total_trip) / sum(d.total_trip) as avg_distance,
    count(distinct d.trip_date) as holiday_days,
    current_date("Asia/Jakarta") as run_date_bq
from daily d
left join holiday_type h
    on d.trip_date = h.date
group by 1,2,3
order by year, month, holiday_type
    );
  