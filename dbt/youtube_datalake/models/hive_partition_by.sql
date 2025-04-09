{{
    config(
        materialized='table',
        unique_key='id',
        partition_by=['city'],
    )
}}

with source_data as (
     select 1 as id, "Name 1" as name, "City 1" as city
     union all
     select 2 as id, "Name 2" as name, "City 2" as city
     union all
     select 3 as id, "Name 3" as name, "City 2" as city
     union all
     select 4 as id, "Name 4" as name, "City 1" as city
)

select * from source_data