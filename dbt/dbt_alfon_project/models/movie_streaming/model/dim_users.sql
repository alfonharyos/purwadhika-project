{{ config(
    materialized='table', 
    tags=['model']
) }}

with users as (
    select
        user_id,
        name,
        email,
        country,
        city,
        join_date,
        subscription_status,
        run_date_bq
    from {{ source('jcdeol004_alfon_movie_streaming_preparation', 'prep_users') }}
),
subscriptions as (
    select 
        user_id, 
        subscription_id
    from {{ source('jcdeol004_alfon_movie_streaming_preparation', 'prep_subscriptions') }}
)

select
    u.user_id,
    u.name,
    u.email,
    u.country,
    u.city,
    s.subscription_id,
    u.join_date,
    u.subscription_status,
    u.run_date_bq
from users u
left join subscriptions s
on u.user_id = s.user_id
