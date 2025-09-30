{{ config(
    materialized='table',
    tags=['model']
) }}

select *
from (
    select 0 as payment_type, 'Flex Fare trip' as payment_desc union all
    select 1, 'Credit card' union all
    select 2, 'Cash' union all
    select 3, 'No charge' union all
    select 4, 'Dispute' union all
    select 5, 'Unknown' union all
    select 6, 'Voided trip'
)
