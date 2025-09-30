-- back compat for old kwarg name
  
  
        
            
            
        
    

    

    merge into `purwadika`.`jcdeol004_alfon_movie_streaming_model`.`fact_user_activity` as DBT_INTERNAL_DEST
        using (


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
    from `purwadika`.`jcdeol004_alfon_movie_streaming_preparation`.`prep_watch_sessions`
    
        where date(start_time) > (select max(date_key) from `purwadika`.`jcdeol004_alfon_movie_streaming_model`.`fact_user_activity`)
    
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
        ) as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.session_id = DBT_INTERNAL_DEST.session_id
            )

    
    when matched then update set
        `session_id` = DBT_INTERNAL_SOURCE.`session_id`,`user_id` = DBT_INTERNAL_SOURCE.`user_id`,`movie_id` = DBT_INTERNAL_SOURCE.`movie_id`,`start_time` = DBT_INTERNAL_SOURCE.`start_time`,`end_time` = DBT_INTERNAL_SOURCE.`end_time`,`watch_duration_minutes` = DBT_INTERNAL_SOURCE.`watch_duration_minutes`,`completed_flag` = DBT_INTERNAL_SOURCE.`completed_flag`,`device_type` = DBT_INTERNAL_SOURCE.`device_type`,`date_key` = DBT_INTERNAL_SOURCE.`date_key`,`run_date_bq` = DBT_INTERNAL_SOURCE.`run_date_bq`
    

    when not matched then insert
        (`session_id`, `user_id`, `movie_id`, `start_time`, `end_time`, `watch_duration_minutes`, `completed_flag`, `device_type`, `date_key`, `run_date_bq`)
    values
        (`session_id`, `user_id`, `movie_id`, `start_time`, `end_time`, `watch_duration_minutes`, `completed_flag`, `device_type`, `date_key`, `run_date_bq`)


    