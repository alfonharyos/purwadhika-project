{{ config(
    materialized='incremental',
    unique_key='rating_id',
    incremental_strategy='merge',
    tags=['model']
) }}

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
    from {{ source('jcdeol004_alfon_movie_streaming_preparation', 'prep_ratings') }}
    {% if is_incremental() %}
      where date(rating_date) > (select max(date_key) from {{ this }})
    {% endif %}
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
