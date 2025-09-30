{{ config(
    materialized='table', 
    tags=['model']
) }}

select
    movie_id,
    title,
    genre,
    release_year,
    duration_min,
    language,
    run_date_bq
from {{ source('jcdeol004_alfon_movie_streaming_preparation', 'prep_movies') }}