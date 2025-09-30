{{ config(
    materialized='incremental',
    unique_key='movie_month_key',
    incremental_strategy='merge',
    partition_by={'field': 'month_key', 'data_type': 'date'},
    cluster_by=['movie_id'], 
    tags=['mart']
) }}

with base as (
    select
        movie_id,
        date_trunc(date_key, month) as month_key,
        total_watch_count,
        unique_viewers,
        avg_watch_duration,
        total_watch_time,
        run_date_bq
    from {{ source('jcdeol004_alfon_movie_streaming_model', 'fact_movie_popularity') }}
    {% if is_incremental() %}
        where date_trunc(date_key, month) >= date_sub(current_date, interval 2 month)
    {% endif %}
),
popularity as (
    select
        movie_id,
        month_key,
        sum(total_watch_count) as total_watch_count,
        sum(unique_viewers) as unique_viewers,
        avg(avg_watch_duration) as avg_watch_duration,
        sum(total_watch_time) as total_watch_time,
        concat(cast(movie_id as string), '_', format_date('%Y%m', month_key)) as movie_month_key,
        max(run_date_bq) as run_date_bq
    from base
    group by movie_id, month_key
)

select
    movie_id,
    month_key,
    total_watch_count,
    unique_viewers,
    avg_watch_duration,
    total_watch_time,
    movie_month_key,
    run_date_bq
from popularity