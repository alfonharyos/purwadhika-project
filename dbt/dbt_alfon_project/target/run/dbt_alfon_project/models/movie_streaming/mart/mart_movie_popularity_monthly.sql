-- back compat for old kwarg name
  
  
        
            
            
        
    

    

    merge into `purwadika`.`jcdeol004_alfon_movie_streaming_mart`.`mart_movie_popularity_monthly` as DBT_INTERNAL_DEST
        using (

with base as (
    select
        movie_id,
        date_trunc(date_key, month) as month_key,
        total_watch_count,
        unique_viewers,
        avg_watch_duration,
        total_watch_time,
        run_date_bq
    from `purwadika`.`jcdeol004_alfon_movie_streaming_model`.`fact_movie_popularity`
    
        where date_trunc(date_key, month) >= date_sub(current_date, interval 2 month)
    
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
        ) as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.movie_month_key = DBT_INTERNAL_DEST.movie_month_key
            )

    
    when matched then update set
        `movie_id` = DBT_INTERNAL_SOURCE.`movie_id`,`month_key` = DBT_INTERNAL_SOURCE.`month_key`,`total_watch_count` = DBT_INTERNAL_SOURCE.`total_watch_count`,`unique_viewers` = DBT_INTERNAL_SOURCE.`unique_viewers`,`avg_watch_duration` = DBT_INTERNAL_SOURCE.`avg_watch_duration`,`total_watch_time` = DBT_INTERNAL_SOURCE.`total_watch_time`,`movie_month_key` = DBT_INTERNAL_SOURCE.`movie_month_key`,`run_date_bq` = DBT_INTERNAL_SOURCE.`run_date_bq`
    

    when not matched then insert
        (`movie_id`, `month_key`, `total_watch_count`, `unique_viewers`, `avg_watch_duration`, `total_watch_time`, `movie_month_key`, `run_date_bq`)
    values
        (`movie_id`, `month_key`, `total_watch_count`, `unique_viewers`, `avg_watch_duration`, `total_watch_time`, `movie_month_key`, `run_date_bq`)


    