

with base as (
    select
        payment_id,
        user_id,
        subscription_id,
        amount,
        payment_date,
        method,
        status,
        created_at,
        run_date_bq
    from `purwadika`.`jcdeol004_alfon_movie_streaming_raw`.`raw_payments`
    
        where run_date_bq > (select max(run_date_bq) from `purwadika`.`jcdeol004_alfon_movie_streaming_preparation`.`prep_payments`)
    
)

select
    payment_id,
    user_id,
    subscription_id,
    amount,
    payment_date,
    method,
    status,
    created_at,
    run_date_bq
from base
qualify row_number() over (partition by payment_id order by created_at desc) = 1