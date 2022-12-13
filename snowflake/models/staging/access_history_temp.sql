{{config(materialized='view',
        tags = ["staging"])}}

with access_history_tmp as (
    select "QUERY_ID"
           -- , (table_object::json->'objectname')::varchar as "object_name"
           -- , table_object:"objectname"::varchar as "object_name"
           , {{json_parse_udf('table_object', "objectName", "varchar", "object_name")}}
    from (
      select
        "QUERY_ID"
        --, (json_array_elements("BASE_OBJECTS_ACCESSED"::json)) as table_object
        --, parse_json(value) as table_object
        , {{json_parse_array_udf("BASE_OBJECTS_ACCESSED", "table_object")}}
      from {{ref('access_history_stage')}}
          {{flatten_json("BASE_OBJECTS_ACCESSED")}}
    ) x
    order by "QUERY_ID"  
)

select "QUERY_ID",
       replace(split_part("object_name", '.',1),'"','') as "database_name",
       replace(split_part("object_name", '.',2),'"','') as "schema_name",
       replace(split_part("object_name", '.',3),'"','') as "table_name"
from access_history_tmp
where "object_name" is not null
  and "table_name" is not null