-- back compat for old kwarg name
  
  
        
            
            
        
    

    

    merge into `purwadika`.`jcdeol004_alfon_movie_streaming_model`.`fact_movie_popularity` as DBT_INTERNAL_DEST
        using (

with base as (
    select
        movie_id,
        date(start_time) as date_key,
        session_id,
        user_id,
        TIMESTAMP_DIFF(end_time, start_time, MINUTE) as watch_duration_minutes,
        run_date_bq
    from `purwadika`.`jcdeol004_alfon_movie_streaming_model`.`fact_user_activity`
    
        where date(start_time) > (select max(date_key) from `purwadika`.`jcdeol004_alfon_movie_streaming_model`.`fact_movie_popularity`)
    
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
        ) as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.movie_id_date = DBT_INTERNAL_DEST.movie_id_date
            )

    
    when matched then update set
        `movie_id` = DBT_INTERNAL_SOURCE.`movie_id`,`date_key` = DBT_INTERNAL_SOURCE.`date_key`,`total_watch_count` = DBT_INTERNAL_SOURCE.`total_watch_count`,`unique_viewers` = DBT_INTERNAL_SOURCE.`unique_viewers`,`avg_watch_duration` = DBT_INTERNAL_SOURCE.`avg_watch_duration`,`total_watch_time` = DBT_INTERNAL_SOURCE.`total_watch_time`,`movie_id_date` = DBT_INTERNAL_SOURCE.`movie_id_date`,`run_date_bq` = DBT_INTERNAL_SOURCE.`run_date_bq`
    

    when not matched then insert
        (`movie_id`, `date_key`, `total_watch_count`, `unique_viewers`, `avg_watch_duration`, `total_watch_time`, `movie_id_date`, `run_date_bq`)
    values
        (`movie_id`, `date_key`, `total_watch_count`, `unique_viewers`, `avg_watch_duration`, `total_watch_time`, `movie_id_date`, `run_date_bq`)


    