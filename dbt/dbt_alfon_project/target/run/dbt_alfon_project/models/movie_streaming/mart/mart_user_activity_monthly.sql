-- back compat for old kwarg name
  
  
        
            
            
        
    

    

    merge into `purwadika`.`jcdeol004_alfon_movie_streaming_mart`.`mart_user_activity_monthly` as DBT_INTERNAL_DEST
        using (

with base as (
    select
        user_id,
        date_trunc(date_key, month) as month_key,
        watch_duration_minutes,
        session_id,
        completed_flag,
        run_date_bq
    from `purwadika`.`jcdeol004_alfon_movie_streaming_model`.`fact_user_activity`
    
        where date_trunc(date_key, month) >= date_sub(current_date, interval 2 month)
    
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
        ) as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.user_month_key = DBT_INTERNAL_DEST.user_month_key
            )

    
    when matched then update set
        `user_id` = DBT_INTERNAL_SOURCE.`user_id`,`month_key` = DBT_INTERNAL_SOURCE.`month_key`,`total_watch_time_minutes` = DBT_INTERNAL_SOURCE.`total_watch_time_minutes`,`total_sessions` = DBT_INTERNAL_SOURCE.`total_sessions`,`avg_watch_duration` = DBT_INTERNAL_SOURCE.`avg_watch_duration`,`completed_sessions` = DBT_INTERNAL_SOURCE.`completed_sessions`,`user_month_key` = DBT_INTERNAL_SOURCE.`user_month_key`,`run_date_bq` = DBT_INTERNAL_SOURCE.`run_date_bq`
    

    when not matched then insert
        (`user_id`, `month_key`, `total_watch_time_minutes`, `total_sessions`, `avg_watch_duration`, `completed_sessions`, `user_month_key`, `run_date_bq`)
    values
        (`user_id`, `month_key`, `total_watch_time_minutes`, `total_sessions`, `avg_watch_duration`, `completed_sessions`, `user_month_key`, `run_date_bq`)


    