{{ config(
    materialized='incremental',
    unique_key='rating_id',
    incremental_strategy='merge', 
    tags=['preparation']
) }}


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
    from {{ source('jcdeol004_alfon_movie_streaming_raw', 'raw_ratings') }}
    {% if is_incremental() %}
        where run_date_bq > (select max(run_date_bq) from {{ this }})
    {% endif %}
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

