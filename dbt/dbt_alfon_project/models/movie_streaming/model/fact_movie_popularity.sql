{{ config(
    materialized='incremental',
    unique_key='movie_id_date',
    incremental_strategy='merge', 
    tags=['model']
) }}

with base as (
    select
        movie_id,
        date(start_time) as date_key,
        session_id,
        user_id,
        TIMESTAMP_DIFF(end_time, start_time, MINUTE) as watch_duration_minutes,
        run_date_bq
    from {{ ref('fact_user_activity') }}
    {% if is_incremental() %}
        where date(start_time) > (select max(date_key) from {{ this }})
    {% endif %}
)

select
    movie_id,
    date_key,
    count(distinct session_id) as total_watch_count,
    count(distinct user_id) as unique_viewers,
    avg(watch_duration_minutes) as avg_watch_duration,
    sum(watch_duration_minutes) as total_watch_time,
    concat(cast(movie_id as string), '_', format_date('%Y%m%d', date_key)) as movie_id_date,
    max(run_date_bq) as run_date_bq
from base
group by movie_id, date_key