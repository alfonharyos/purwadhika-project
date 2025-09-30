{{ config(
    materialized='table', 
    tags=['model']
) }}

select
    subscription_id,
    user_id,
    plan_type,
    start_date,
    end_date,
    price,
    status,
    run_date_bq
from {{ source('jcdeol004_alfon_movie_streaming_preparation', 'prep_subscriptions') }}