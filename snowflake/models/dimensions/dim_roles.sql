{{
  config(
    materialized='incremental',
    unique_key = '"ROLES_ID"',
    merge_update_columns = ['var("col_update_dts")','CREATED_ON','NAME','ROLE_NAME'],
    tags = ["dimensions"]
  )
}}

with roles as (
    SELECT * 
    FROM {{ref('roles_stage')}}
),

query_history as (
    select * 
    from {{ref('query_history_stage')}}
),

dimension as (
    SELECT DISTINCT
            current_timestamp as "{{var('col_create_dts')}}",
            current_timestamp as "{{var('col_update_dts')}}",
           COALESCE (r."CREATED_ON", 'N/A') AS "CREATED_ON",
           COALESCE (r."DELETED_ON", 'N/A') AS "DELETED_ON",
           CASE WHEN r."NAME" IS NOT NULL THEN R."NAME"
                WHEN qh."ROLE_NAME" IS NOT NULL THEN QH."ROLE_NAME"
                ELSE 'N/A' END AS "NAME",
           COALESCE (r."COMMENT", 'N/A') AS "COMMENT"
    FROM query_history qh
    FULL OUTER JOIN roles r ON R."NAME" = QH."ROLE_NAME"
)

SELECT      
    {{dbt_utils.generate_surrogate_key(
         ['"NAME"']
     )}} AS "ROLES_ID", *
FROM dimension