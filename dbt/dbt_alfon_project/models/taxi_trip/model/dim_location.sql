{{ config(
    materialized='table',
    tags=['model']
) }}


select
    locationid,
    borough,
    zone,
    service_zone
from {{ source('taxi_zone', 'raw_taxi_zone') }}
