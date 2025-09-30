{{ config(
    materialized='incremental',
    unique_key='subscription_month_key',
    incremental_strategy='merge',
    partition_by={'field': 'month_key', 'data_type': 'date'},
    cluster_by=['subscription_id'],
    tags=['mart']
) }}

-- Base payments per subscription
with base as (
    select
        subscription_id,
        date_trunc(date(payment_date), month) as month_key,
        amount,
        payment_id,
        status,
        run_date_bq
    from {{ source('jcdeol004_alfon_movie_streaming_model', 'fact_payments') }}
    {% if is_incremental() %}
        where date_trunc(date(payment_date), month) >= date_sub(current_date, interval 2 month)
    {% endif %}
),

-- Aggregate monthly revenue
revenue as (
    select
        subscription_id,
        month_key,
        sum(amount) as total_amount,
        count(payment_id) as total_payments,
        sum(case when status='completed' then 1 else 0 end) as status_completed,
        sum(case when status='pending' then 1 else 0 end) as status_pending,
        sum(case when status='failed' then 1 else 0 end) as status_failed,
        concat(cast(subscription_id as string), '_', format_date('%Y%m', month_key)) as subscription_month_key,
        max(run_date_bq) as run_date_bq
    from base
    group by subscription_id, month_key
),

-- Calculate active, new, and churned users per month
user_activity as (
    select
        month_key,
        count(distinct subscription_id) as total_active_users,
        count(distinct case when first_payment_date = month_key then subscription_id end) as new_users,
        count(distinct case when last_payment_date < month_key then subscription_id end) as churned_users
    from (
        select
            subscription_id,
            month_key,
            min(month_key) over (partition by subscription_id) as first_payment_date,
            max(month_key) over (partition by subscription_id) as last_payment_date
        from revenue
    )
    group by month_key
)

select
    r.subscription_id,
    r.month_key,
    r.total_amount,
    r.total_payments,
    r.status_completed,
    r.status_pending,
    r.status_failed,
    r.subscription_month_key,
    r.run_date_bq,
    ua.total_active_users,
    ua.new_users,
    ua.churned_users
from revenue r
left join user_activity ua
    on r.month_key = ua.month_key
