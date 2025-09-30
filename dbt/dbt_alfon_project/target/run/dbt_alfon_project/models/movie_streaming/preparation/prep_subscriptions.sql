-- back compat for old kwarg name
  
  
        
            
            
        
    

    

    merge into `purwadika`.`jcdeol004_alfon_movie_streaming_preparation`.`prep_subscriptions` as DBT_INTERNAL_DEST
        using (

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
    from `purwadika`.`jcdeol004_alfon_movie_streaming_raw`.`raw_subscriptions`
    
        where run_date_bq > (select max(run_date_bq) from `purwadika`.`jcdeol004_alfon_movie_streaming_preparation`.`prep_subscriptions`)
    
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
        ) as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.subscription_id = DBT_INTERNAL_DEST.subscription_id
            )

    
    when matched then update set
        `subscription_id` = DBT_INTERNAL_SOURCE.`subscription_id`,`user_id` = DBT_INTERNAL_SOURCE.`user_id`,`plan_type` = DBT_INTERNAL_SOURCE.`plan_type`,`start_date` = DBT_INTERNAL_SOURCE.`start_date`,`end_date` = DBT_INTERNAL_SOURCE.`end_date`,`price` = DBT_INTERNAL_SOURCE.`price`,`status` = DBT_INTERNAL_SOURCE.`status`,`created_at` = DBT_INTERNAL_SOURCE.`created_at`,`run_date_bq` = DBT_INTERNAL_SOURCE.`run_date_bq`
    

    when not matched then insert
        (`subscription_id`, `user_id`, `plan_type`, `start_date`, `end_date`, `price`, `status`, `created_at`, `run_date_bq`)
    values
        (`subscription_id`, `user_id`, `plan_type`, `start_date`, `end_date`, `price`, `status`, `created_at`, `run_date_bq`)


    