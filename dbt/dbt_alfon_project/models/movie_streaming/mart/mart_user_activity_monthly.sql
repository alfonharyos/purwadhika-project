{{ config(
    materialized='incremental',
    unique_key='user_month_key',
    incremental_strategy='merge',
    partition_by={'field': 'month_key', 'data_type': 'date'},
    cluster_by=['user_id'], 
    tags=['mart']
) }}

with base as (
    select
        user_id,
        date_trunc(date_key, month) as month_key,
        watch_duration_minutes,
        session_id,
        completed_flag,
        run_date_bq
    from {{ source('jcdeol004_alfon_movie_streaming_model', 'fact_user_activity') }}
    {% if is_incremental() %}
        where date_trunc(date_key, month) >= date_sub(current_date, interval 2 month)
    {% endif %}
),

activity as (
    select
        user_id,
        month_key,
        sum(watch_duration_minutes) as total_watch_time_minutes,
        count(session_id) as total_sessions,
        avg(watch_duration_minutes) as avg_watch_duration,
        sum(case when completed_flag then 1 else 0 end) as completed_sessions,
        concat(cast(user_id as string), '_', format_date('%Y%m', month_key)) as user_month_key,
        max(run_date_bq) as run_date_bq
    from base
    group by user_id, month_key
)

select
    user_id,
    month_key,
    total_watch_time_minutes,
    total_sessions,
    avg_watch_duration,
    completed_sessions,
    user_month_key,
    run_date_bq
from activity