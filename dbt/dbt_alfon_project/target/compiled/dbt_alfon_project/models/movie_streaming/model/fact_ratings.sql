

with base as (
    select
        rating_id,
        user_id,
        movie_id,
        rating_score,
        rating_date,
        review_text,
        date(rating_date) as date_key,
        run_date_bq,
        created_at,
        row_number() over (partition by rating_id order by created_at desc) as rn
    from `purwadika`.`jcdeol004_alfon_movie_streaming_preparation`.`prep_ratings`
    
      where date(rating_date) > (select max(date_key) from `purwadika`.`jcdeol004_alfon_movie_streaming_model`.`fact_ratings`)
    
)

select
    rating_id,
    user_id,
    movie_id,
    rating_score,
    rating_date,
    review_text,
    date_key,
    run_date_bq
from base
where rn = 1