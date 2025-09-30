{{ config(
    materialized='incremental',
    unique_key='session_id',
    incremental_strategy='merge', 
    tags=['preparation']
) }}

with base as (
    select
        session_id,
        user_id,
        movie_id,
        device_type,
        start_time,
        end_time,
        completion_rate,
        created_at,
        run_date_bq
    from {{ source('jcdeol004_alfon_movie_streaming_raw', 'raw_watch_sessions') }}
    {% if is_incremental() %}
        where run_date_bq > (select max(run_date_bq) from {{ this }})
    {% endif %}
)

select
    session_id,
    user_id,
    movie_id,
    device_type,
    start_time,
    end_time,
    completion_rate,
    created_at,
    run_date_bq
from base
qualify row_number() over (partition by session_id order by created_at desc) = 1