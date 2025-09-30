

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
    from `purwadika`.`jcdeol004_alfon_movie_streaming_preparation`.`prep_payments`
    
        where date(payment_date) > (select max(date_key) from `purwadika`.`jcdeol004_alfon_movie_streaming_model`.`fact_payments`)
    
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