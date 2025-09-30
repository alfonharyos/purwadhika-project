-- back compat for old kwarg name
  
  
        
            
            
        
    

    

    merge into `purwadika`.`jcdeol004_alfon_movie_streaming_preparation`.`prep_ratings` as DBT_INTERNAL_DEST
        using (


with base as (
    select
        rating_id,
        user_id,
        movie_id,
        rating_score,
        rating_date,
        review_text,
        created_at,
        run_date_bq
    from `purwadika`.`jcdeol004_alfon_movie_streaming_raw`.`raw_ratings`
    
        where run_date_bq > (select max(run_date_bq) from `purwadika`.`jcdeol004_alfon_movie_streaming_preparation`.`prep_ratings`)
    
)

select
    rating_id,
    user_id,
    movie_id,
    rating_score,
    rating_date,
    review_text,
    created_at,
    run_date_bq
from base
qualify row_number() over (partition by rating_id order by created_at desc) = 1
        ) as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.rating_id = DBT_INTERNAL_DEST.rating_id
            )

    
    when matched then update set
        `rating_id` = DBT_INTERNAL_SOURCE.`rating_id`,`user_id` = DBT_INTERNAL_SOURCE.`user_id`,`movie_id` = DBT_INTERNAL_SOURCE.`movie_id`,`rating_score` = DBT_INTERNAL_SOURCE.`rating_score`,`rating_date` = DBT_INTERNAL_SOURCE.`rating_date`,`review_text` = DBT_INTERNAL_SOURCE.`review_text`,`created_at` = DBT_INTERNAL_SOURCE.`created_at`,`run_date_bq` = DBT_INTERNAL_SOURCE.`run_date_bq`
    

    when not matched then insert
        (`rating_id`, `user_id`, `movie_id`, `rating_score`, `rating_date`, `review_text`, `created_at`, `run_date_bq`)
    values
        (`rating_id`, `user_id`, `movie_id`, `rating_score`, `rating_date`, `review_text`, `created_at`, `run_date_bq`)


    