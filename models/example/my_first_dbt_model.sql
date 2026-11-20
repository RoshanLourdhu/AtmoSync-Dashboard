{{ config(materialized='table') }}

with source_data as (

    select 1 as id, 'Hello dbt' as message
    union all
    select null as id, null as message

)

select *
from source_data
where id is not null