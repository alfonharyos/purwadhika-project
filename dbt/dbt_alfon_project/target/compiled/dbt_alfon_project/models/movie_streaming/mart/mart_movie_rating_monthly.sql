

with base as (
    select
        movie_id,
        date_trunc(date_key, month) as month_key,
        rating_score,
        rating_id,
        user_id,
        run_date_bq
    from `purwadika`.`jcdeol004_alfon_movie_streaming_model`.`fact_ratings`
    
        where date_trunc(date_key, month) >= date_sub(current_date, interval 2 month)
    
),

rating as (
    select
        movie_id,
        month_key,
        avg(rating_score) as avg_rating,
        count(rating_id) as total_ratings,
        count(distinct user_id) as unique_raters,
        concat(cast(movie_id as string), '_', format_date('%Y%m', month_key)) as movie_month_key,
        max(run_date_bq) as run_date_bq
    from base
    group by movie_id, month_key
)

select
    movie_id,
    month_key,
    avg_rating,
    total_ratings,
    unique_raters,
    movie_month_key,
    run_date_bq
from rating