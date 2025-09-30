

select
    movie_id,
    initcap(trim(title)) as title,
    initcap(trim(genre)) as genre,
    release_year,
    duration_min,
    initcap(language) as language,
    created_at,
    run_date_bq
from `purwadika`.`jcdeol004_alfon_movie_streaming_raw`.`raw_movies`

qualify row_number() over (partition by movie_id order by created_at desc) = 1