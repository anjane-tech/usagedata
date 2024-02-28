{{
  config(
    materialized='incremental',
    unique_key = '"ERROR_ID"',
    merge_update_columns = [var("col_update_dts"),'ERROR_CODE','DESCRIPTION','ERROR_TYPE'],
    tags = ["dimensions"],
    schema = var("usage_data_entityattribute_schema_name")
  )
}}

with errors as (
    SELECT *
    FROM {{ref('errors_stage_vw')}}
    UNION ALL
    SELECT '-' AS "ERROR_TYPE",
           0 AS "ERROR_CODE",
           'Not Available' AS "DESCRIPTION"

),

query_history as (
    select * 
    from {{ref('query_history_stage')}}
),

dimensions as (
    SELECT DISTINCT
           COALESCE (e."ERROR_TYPE", 'N/A') AS "ERROR_TYPE",
           CASE WHEN NULLIF(TRIM(e."ERROR_CODE"),'') IS NOT NULL THEN E."ERROR_CODE"
                WHEN NULLIF(TRIM(qh."ERROR_CODE"), '') IS NOT NULL THEN QH."ERROR_CODE"
                ELSE 0 END AS "ERROR_CODE",           
           CASE WHEN NULLIF(TRIM(e."DESCRIPTION"),'') IS NOT NULL THEN E."DESCRIPTION"
                WHEN NULLIF(TRIM(qh."ERROR_MESSAGE"),'') IS NOT NULL THEN QH."ERROR_MESSAGE"
                ELSE 'N/A' END AS "DESCRIPTION"
    FROM query_history qh
    FULL OUTER JOIN errors e on E."ERROR_CODE" = COALESCE(QH."ERROR_CODE",0)
)

SELECT   
    'error' as entity_name,
    {{dbt_utils.generate_surrogate_key(
         ['"ERROR_CODE"']
     )}} AS id,
    ERROR_TYPE as  attribute_1,
    ERROR_CODE as attribute_2,
    DESCRIPTION as attribute_3,
    '' as attribute_4,
    '' as attribute_5,
    '' as attribute_6,
    '' as attribute_7,
    '' as attribute_8,
    '' as attribute_9,
    '' as attribute_10,
    current_timestamp as "{{var('col_create_dts')}}",
    current_timestamp as "{{var('col_update_dts')}}"
FROM dimensions