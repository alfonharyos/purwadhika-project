-- back compat for old kwarg name
  
  
        
            
            
        
    

    

    merge into `purwadika`.`jcdeol004_alfon_movie_streaming_preparation`.`prep_payments` as DBT_INTERNAL_DEST
        using (

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
        ) as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.payment_id = DBT_INTERNAL_DEST.payment_id
            )

    
    when matched then update set
        `payment_id` = DBT_INTERNAL_SOURCE.`payment_id`,`user_id` = DBT_INTERNAL_SOURCE.`user_id`,`subscription_id` = DBT_INTERNAL_SOURCE.`subscription_id`,`amount` = DBT_INTERNAL_SOURCE.`amount`,`payment_date` = DBT_INTERNAL_SOURCE.`payment_date`,`method` = DBT_INTERNAL_SOURCE.`method`,`status` = DBT_INTERNAL_SOURCE.`status`,`created_at` = DBT_INTERNAL_SOURCE.`created_at`,`run_date_bq` = DBT_INTERNAL_SOURCE.`run_date_bq`
    

    when not matched then insert
        (`payment_id`, `user_id`, `subscription_id`, `amount`, `payment_date`, `method`, `status`, `created_at`, `run_date_bq`)
    values
        (`payment_id`, `user_id`, `subscription_id`, `amount`, `payment_date`, `method`, `status`, `created_at`, `run_date_bq`)


    