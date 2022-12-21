{{
  config(
    materialized='incremental',
    unique_key = '"ROLES_ID"',
    merge_update_columns = [var("col_update_dts"),'CREATED_ON','NAME'],
    tags = ["dimensions"]
  )
}}

with roles as (
    SELECT * 
    FROM {{ref('roles_stage_vw')}}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY NAME ORDER BY CREATED_ON DESC) = 1
),

query_history as (
    select * 
    from {{ref('query_history_stage')}}
),

dimension as (
    SELECT DISTINCT
           COALESCE (r."CREATED_ON", to_timestamp_ntz('1901-01-01')) AS "CREATED_ON",
           COALESCE (r."DELETED_ON", to_timestamp_ntz('1901-01-01')) AS "DELETED_ON",
           CASE WHEN NULLIF(TRIM(r."NAME"),'') IS NOT NULL THEN R."NAME"
                WHEN NULLIF(TRIM(qh."ROLE_NAME"),'') IS NOT NULL THEN QH."ROLE_NAME"
                ELSE 'N/A' END AS "NAME",
           COALESCE (r."COMMENT", 'N/A') AS "COMMENT",
           COALESCE (r."OWNER", 'N/A') AS "OWNER"
    FROM query_history qh
    FULL OUTER JOIN roles r ON R."NAME" = QH."ROLE_NAME"
)

SELECT      
    {{dbt_utils.generate_surrogate_key(
         ['"NAME"']
     )}} AS "ROLES_ID", *,
     current_timestamp as "{{var('col_create_dts')}}",
      current_timestamp as "{{var('col_update_dts')}}"
FROM dimension
