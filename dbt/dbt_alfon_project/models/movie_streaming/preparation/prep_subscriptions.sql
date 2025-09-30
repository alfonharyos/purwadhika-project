{{ config(
    materialized='incremental',
    unique_key='subscription_id',
    incremental_strategy='merge', 
    tags=['preparation']
) }}

with base as (
    select
        subscription_id,
        user_id,
        plan_type,
        start_date,
        end_date,
        price,
        status,
        created_at,
        run_date_bq
    from {{ source('jcdeol004_alfon_movie_streaming_raw', 'raw_subscriptions') }}
    {% if is_incremental() %}
        where run_date_bq > (select max(run_date_bq) from {{ this }})
    {% endif %}
)

select
    subscription_id,
    user_id,
    plan_type,
    start_date,
    end_date,
    price,
    status,
    created_at,
    run_date_bq
from base
qualify row_number() over (partition by subscription_id order by created_at desc) = 1

