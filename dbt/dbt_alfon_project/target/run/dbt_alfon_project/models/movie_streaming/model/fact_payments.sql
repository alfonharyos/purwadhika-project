-- back compat for old kwarg name
  
  
        
            
            
        
    

    

    merge into `purwadika`.`jcdeol004_alfon_movie_streaming_model`.`fact_payments` as DBT_INTERNAL_DEST
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
        ) as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.payment_id = DBT_INTERNAL_DEST.payment_id
            )

    
    when matched then update set
        `payment_id` = DBT_INTERNAL_SOURCE.`payment_id`,`user_id` = DBT_INTERNAL_SOURCE.`user_id`,`subscription_id` = DBT_INTERNAL_SOURCE.`subscription_id`,`amount` = DBT_INTERNAL_SOURCE.`amount`,`payment_date` = DBT_INTERNAL_SOURCE.`payment_date`,`method` = DBT_INTERNAL_SOURCE.`method`,`status` = DBT_INTERNAL_SOURCE.`status`,`date_key` = DBT_INTERNAL_SOURCE.`date_key`,`run_date_bq` = DBT_INTERNAL_SOURCE.`run_date_bq`
    

    when not matched then insert
        (`payment_id`, `user_id`, `subscription_id`, `amount`, `payment_date`, `method`, `status`, `date_key`, `run_date_bq`)
    values
        (`payment_id`, `user_id`, `subscription_id`, `amount`, `payment_date`, `method`, `status`, `date_key`, `run_date_bq`)


    