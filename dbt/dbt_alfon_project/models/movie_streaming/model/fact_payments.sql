{{ config(
    materialized='incremental',
    unique_key='payment_id',
    incremental_strategy='merge',
    tags=['model']
) }}

with base as (
    select
        payment_id,
        user_id,
        subscription_id,
        amount,
        payment_date,
        method,
        status,
        date(payment_date) as date_key,
        run_date_bq,
        created_at,
        row_number() over (partition by payment_id order by created_at desc) as rn
    from {{ source('jcdeol004_alfon_movie_streaming_preparation', 'prep_payments') }}
    {% if is_incremental() %}
        where date(payment_date) > (select max(date_key) from {{ this }})
    {% endif %}
)

select
    payment_id,
    user_id,
    subscription_id,
    amount,
    payment_date,
    method,
    status,
    date_key,
    run_date_bq
from base
where rn = 1