{{ config(
    materialized='incremental',
    unique_key='session_id',
    incremental_strategy='merge', 
    tags=['model']
) }}


with base as (
    select
        session_id,
        user_id,
        movie_id,
        start_time,
        end_time,
        TIMESTAMP_DIFF(end_time, start_time, MINUTE) as watch_duration_minutes,
        case 
            when completion_rate >= 0.85 then true
            else false
        end as completed_flag,
        device_type,
        date(start_time) as date_key,
        run_date_bq,
        row_number() over (partition by session_id order by created_at desc) as rn
    from {{ source('jcdeol004_alfon_movie_streaming_preparation', 'prep_watch_sessions') }}
    {% if is_incremental() %}
        where date(start_time) > (select max(date_key) from {{ this }})
    {% endif %}
)

select
    session_id,
    user_id,
    movie_id,
    start_time,
    end_time,
    watch_duration_minutes,
    completed_flag,
    device_type,
    date_key,
    run_date_bq
from base
where rn = 1



