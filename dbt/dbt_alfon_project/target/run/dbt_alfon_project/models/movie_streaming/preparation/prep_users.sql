
  
    

    create or replace table `purwadika`.`jcdeol004_alfon_movie_streaming_preparation`.`prep_users`
      
    
    

    OPTIONS()
    as (
      

select
    user_id,
    initcap(trim(name)) as name,
    lower(trim(email)) as email,
    initcap(trim(country)) as country,
    initcap(trim(city)) as city,
    date(join_date) as join_date,
    subscription_status,
    created_at,
    run_date_bq
from `purwadika`.`jcdeol004_alfon_movie_streaming_raw`.`raw_users`

qualify row_number() over (partition by user_id order by created_at desc) = 1
    );
  