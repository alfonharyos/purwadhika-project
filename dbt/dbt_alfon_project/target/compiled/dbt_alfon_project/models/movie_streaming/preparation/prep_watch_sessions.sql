

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
    from `purwadika`.`jcdeol004_alfon_movie_streaming_raw`.`raw_watch_sessions`
    
        where run_date_bq > (select max(run_date_bq) from `purwadika`.`jcdeol004_alfon_movie_streaming_preparation`.`prep_watch_sessions`)
    
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