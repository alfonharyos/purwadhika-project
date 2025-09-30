-- back compat for old kwarg name
  
  
        
            
            
        
    

    

    merge into `purwadika`.`jcdeol004_alfon_movie_streaming_model`.`fact_ratings` as DBT_INTERNAL_DEST
        using (

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
        ) as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.rating_id = DBT_INTERNAL_DEST.rating_id
            )

    
    when matched then update set
        `rating_id` = DBT_INTERNAL_SOURCE.`rating_id`,`user_id` = DBT_INTERNAL_SOURCE.`user_id`,`movie_id` = DBT_INTERNAL_SOURCE.`movie_id`,`rating_score` = DBT_INTERNAL_SOURCE.`rating_score`,`rating_date` = DBT_INTERNAL_SOURCE.`rating_date`,`review_text` = DBT_INTERNAL_SOURCE.`review_text`,`date_key` = DBT_INTERNAL_SOURCE.`date_key`,`run_date_bq` = DBT_INTERNAL_SOURCE.`run_date_bq`
    

    when not matched then insert
        (`rating_id`, `user_id`, `movie_id`, `rating_score`, `rating_date`, `review_text`, `date_key`, `run_date_bq`)
    values
        (`rating_id`, `user_id`, `movie_id`, `rating_score`, `rating_date`, `review_text`, `date_key`, `run_date_bq`)


    