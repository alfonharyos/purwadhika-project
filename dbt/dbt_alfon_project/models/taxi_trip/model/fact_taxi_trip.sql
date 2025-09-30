{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='md5_key',
    partition_by={"field": "pickup_datetime", "data_type": "timestamp", "granularity": "month"},
    cluster_by=["taxi_type", "pulocationid"],
    tags=['model']
) }}

with prep as (
    select *
    from {{ ref('prep_taxi_trip') }}
    {% if is_incremental() %}
        where date(date_trunc(pickup_datetime, month)) >= (
            select date_sub(
                coalesce(date(max(date_trunc(pickup_datetime, month))), date('2023-01-01')),
                interval 2 month
            )
            from {{ this }}
        )
    {% endif %}
)

select
    md5_key,
    taxi_type,
    pickup_datetime,
    dropoff_datetime,
    pulocationid,
    dolocationid,
    passenger_count,
    cast(payment_type as int64) as payment_type,
    fare_amount,
    tip_amount,
    total_amount,
    trip_distance,
    current_date("Asia/Jakarta") as run_date_bq
from prep
