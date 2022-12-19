{{
  config(
    materialized='incremental',
    unique_key = '"COMBINED_USERS_ID"',
    merge_update_columns = [var("col_update_dts"),'NAME','LOGIN_NAME','EMAIL','HAS_PASSWORD','EXPIRES_AT','DISPLAY_NAME'],
    tags = ["dimensions"]
  )
}}

with users as (
    SELECT * 
    FROM {{ref('users_stage_vw')}}
),

query_history as (
    select * 
    from {{ref('query_history_stage')}}
),

dimension as (
    SELECT DISTINCT
           CASE WHEN u."NAME" IS NOT NULL THEN U."NAME"
                WHEN qh."USER_NAME" IS NOT NULL THEN QH."USER_NAME"
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
    {{dbt_utils.generate_surrogate_key(
         ['"NAME"']
     )}} AS "COMBINED_USERS_ID", *,
     current_timestamp as "{{var('col_create_dts')}}",
      current_timestamp as "{{var('col_update_dts')}}"
FROM dimension

