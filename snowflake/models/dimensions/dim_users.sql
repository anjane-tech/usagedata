{{
  config(
    materialized='incremental',
    unique_key = '"COMBINED_USERS_ID"',
    merge_update_columns = ['var("col_update_dts")','USER_NAME','LOGIN_NAME','EMAIL','HAS_PASSWORD','EXPIRES_AT','DISPLAY_NAME'],
    tags = ["dimensions"]
  )
}}

with users as (
    SELECT * 
    FROM {{ref('users_stage')}}
),

query_history as (
    select * 
    from {{ref('query_history_stage')}}
),

dimension as (
    SELECT DISTINCT
            current_timestamp as "{{var('col_create_dts')}}",
            current_timestamp as "{{var('col_update_dts')}}",
           CASE WHEN u."NAME" IS NOT NULL THEN U."NAME"
                WHEN qh."USER_NAME" IS NOT NULL THEN QH."USER_NAME"
                ELSE 'N/A' END AS "NAME",
           COALESCE (u."CREATED_ON", 'N/A') AS "CREATED_ON",
           COALESCE (u."DELETED_ON", 'N/A') AS "DELETED_ON",
           COALESCE (u."LOGIN_NAME", 'N/A') AS "LOGIN_NAME",
           COALESCE (u."EMAIL", 'N/A') AS "EMAIL",
           COALESCE (u."HAS_PASSWORD", 'N/A') AS "HAS_PASSWORD",
           COALESCE (u."DEFAULT_ROLE", 'N/A') AS "DEFAULT_ROLE",
           COALESCE (u."EXPIRES_AT", 'N/A') AS "EXPIRES_AT",
           COALESCE (u."DISPLAY_NAME", 'N/A') AS "DISPLAY_NAME"
    FROM query_history qh
    FULL OUTER JOIN users u ON U."NAME" = QH."USER_NAME"
)

SELECT      
    {{dbt_utils.generate_surrogate_key(
         ['"NAME"']
     )}} AS "COMBINED_USERS_ID", *
FROM dimension

