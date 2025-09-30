-- back compat for old kwarg name
  
  
        
            
            
        
    

    

    merge into `purwadika`.`jcdeol004_alfon_movie_streaming_preparation`.`prep_watch_sessions` as DBT_INTERNAL_DEST
        using (

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
        ) as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.session_id = DBT_INTERNAL_DEST.session_id
            )

    
    when matched then update set
        `session_id` = DBT_INTERNAL_SOURCE.`session_id`,`user_id` = DBT_INTERNAL_SOURCE.`user_id`,`movie_id` = DBT_INTERNAL_SOURCE.`movie_id`,`device_type` = DBT_INTERNAL_SOURCE.`device_type`,`start_time` = DBT_INTERNAL_SOURCE.`start_time`,`end_time` = DBT_INTERNAL_SOURCE.`end_time`,`completion_rate` = DBT_INTERNAL_SOURCE.`completion_rate`,`created_at` = DBT_INTERNAL_SOURCE.`created_at`,`run_date_bq` = DBT_INTERNAL_SOURCE.`run_date_bq`
    

    when not matched then insert
        (`session_id`, `user_id`, `movie_id`, `device_type`, `start_time`, `end_time`, `completion_rate`, `created_at`, `run_date_bq`)
    values
        (`session_id`, `user_id`, `movie_id`, `device_type`, `start_time`, `end_time`, `completion_rate`, `created_at`, `run_date_bq`)


    