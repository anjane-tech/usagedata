{{
  config(
    materialized='incremental',
    unique_key = '"COMBINED_USERS_ID"',
    merge_update_columns = [var("col_update_dts"),'NAME','LOGIN_NAME','EMAIL','HAS_PASSWORD','EXPIRES_AT','DISPLAY_NAME'],
    tags = ["dimensions"],
    schema = var("usage_data_entityattribute_schema_name")
  )
}}

with users as (
    SELECT * 
    FROM {{ref('users_stage_vw')}}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY "NAME" ORDER BY CREATED_ON DESC) = 1
),

query_history as (
    select * 
    from {{ref('query_history_stage')}}
),

dimension as (
    SELECT DISTINCT
           CASE WHEN NULLIF(TRIM(u."NAME"),'') IS NOT NULL THEN U."NAME"
                WHEN NULLIF(TRIM(qh."USER_NAME"),'') IS NOT NULL THEN QH."USER_NAME"
                ELSE 'N/A' END AS "NAME",
           COALESCE (u."CREATED_ON", to_timestamp_ntz('1901-01-01')) AS "CREATED_ON",
           COALESCE (u."DELETED_ON", to_timestamp_ntz('1901-01-01')) AS "DELETED_ON",
           COALESCE (u."LOGIN_NAME", 'N/A') AS "LOGIN_NAME",
           COALESCE (u."EMAIL", 'N/A') AS "EMAIL",
           u."HAS_PASSWORD",
           COALESCE (u."DEFAULT_ROLE", 'N/A') AS "DEFAULT_ROLE",
           COALESCE (u."EXPIRES_AT", to_timestamp_ntz('1901-01-01')) AS "EXPIRES_AT",
           COALESCE (u."DISPLAY_NAME", 'N/A') AS "DISPLAY_NAME"
    FROM query_history qh
    FULL OUTER JOIN users u ON U."NAME" = QH."USER_NAME"
)

SELECT      
     'users' as entity_name,

    {{dbt_utils.generate_surrogate_key(
         ['"NAME"']
     )}} AS id,
     NAME as attribute_1,
    
     CREATED_ON as attribute_2,
     DELETED_ON as attribute_3,
     LOGIN_NAME as attribute_4,
     EMAIL as attribute_5,
     DEFAULT_ROLE as attribute_6,
     EXPIRES_AT as attribute_7,
     DISPLAY_NAME as attribute_8,
     '' as attribute_9,
     '' as attribute_10,

     current_timestamp as "{{var('col_create_dts')}}",
      current_timestamp as "{{var('col_update_dts')}}"
FROM dimension

