{{
    config(
        materialized='table',
        partition_by=['city'],
        file_format='PARQUET',
        location='/datalake/teste',
        external=true,
    )
}}

with source_data as (
    select 1 as id, 'Alice' as name, 'New York' as city
    union all
    select 2 as id, 'Bob' as name, 'San Francisco' as city
    union all
    select 3 as id, 'Carol' as name, 'New York' as city
    union all
    select 4 as id, 'David' as name, 'San Francisco' as city
)

select id, name, city
from source_data