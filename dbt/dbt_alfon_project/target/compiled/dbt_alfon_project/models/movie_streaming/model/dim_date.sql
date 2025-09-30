

with calendar as (
    select
        date_add('1970-01-01', interval seq day) as date
    from unnest(generate_array(0, 365*10)) as seq
)
select
    date as date_key,
    date as full_date,
    extract(dayofweek from date) as day_of_week,
    case when extract(dayofweek from date) in (1,7) then true else false end as is_weekend,
    extract(month from date) as month,
    format_date('%b', date) as month_name,
    extract(quarter from date) as quarter,
    extract(year from date) as year
from calendar