{{
  config(
    materialized='incremental',
    unique_key = '"ERROR_ID"',
    merge_update_columns = ['var("col_update_dts")','ERROR_CODE','ERROR_MESSAGE','ERROR_TYPE'],
    tags = ["dimensions"]
  )
}}

with errors as (
    SELECT *
    FROM {{ref('errors_stage_vw')}}
),

query_history as (
    select * 
    from {{ref('query_history_stage_vw')}}
),

dimensions as (
    SELECT DISTINCT
           COALESCE (e."ERROR_TYPE", 'N/A') AS "ERROR_TYPE",
           CASE WHEN e."ERROR_CODE" IS NOT NULL THEN E."ERROR_CODE"
                WHEN qh."ERROR_CODE" IS NOT NULL THEN QH."ERROR_CODE"
                ELSE 0 END AS "ERROR_CODE",           
           CASE WHEN e."DESCRIPTION" IS NOT NULL THEN E."DESCRIPTION"
                WHEN qh."ERROR_MESSAGE" IS NOT NULL THEN QH."ERROR_MESSAGE"
                ELSE 'N/A' END AS "DESCRIPTION"
    FROM query_history qh
    FULL OUTER JOIN errors e on E."ERROR_CODE" = QH."ERROR_CODE"
)

SELECT      
    {{dbt_utils.generate_surrogate_key(
         ['"ERROR_CODE"']
     )}} AS "ERROR_ID", *,
     current_timestamp as "{{var('col_create_dts')}}",
      current_timestamp as "{{var('col_update_dts')}}"
FROM dimensions