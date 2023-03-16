{{
  config(
    materialized='incremental',
    unique_key = '"METERING_HISTORY_ID"',
    merge_update_columns = [var("col_update_dts"),SERVICE_TYPE,START_TIME,END_TIME,ENTITY_ID,CREDITS_USED_COMPUTE,NAME,CREDITS_USED_CLOUD_SERVICES,CREDITS_USED],
    tags = ["dimensions"],
    schema = var("usage_data_datamart_schema_name")
  )
}}

with metering_history as (
    select * from {{ref('metering_history_stage_vw')}}
),

query_history as (
    SELECT * 
    FROM {{ref('query_history_stage')}}
),

dimensions as (
        SELECT DISTINCT
                COALESCE(mh."SERVICE_TYPE", 'N/A') as "SERVICE_TYPE",
                CASE WHEN mh."START_TIME" IS NOT NULL THEN MH."START_TIME"
                    WHEN qh."START_TIME" IS NOT NULL THEN QH."START_TIME"
                    ELSE "1909-01-01" END AS "START_TIME",
                CASE WHEN mh."END_TIME" IS NOT NULL THEN MH."END_TIME"
                    WHEN qh."END_TIME" IS NOT NULL THEN QH."END_TIME"
                    ELSE "1909-01-01" END AS "END_TIME",
                COALESCE(mh."ENTITY_ID", 'N/A') as "ENTITY_ID",
                CASE WHEN NULLIF(TRIM(mh."NAME"),'') IS NOT NULL THEN MH."NAME"
                     WHEN NULLIF(TRIM(qh."USER_NAME"),'') IS NOT NULL THEN QH."USER_NAME"
                     ELSE 'N/A' END AS "NAME",
                COALESCE(mh."CREDITS_USED_COMPUTE", 'N/A') as "CREDITS_USED_COMPUTE",
                CASE WHEN NULLIF(TRIM(mh."CREDITS_USED_CLOUD_SERVICES"),'') IS NOT NULL THEN MH."CREDITS_USED_CLOUD_SERVICES"
                     WHEN NULLIF(TRIM(qh."CREDITS_USED_CLOUD_SERVICES"),'') IS NOT NULL THEN QH."CREDITS_USED_CLOUD_SERVICES"
                    ELSE "N/A" END AS "CREDITS_USED_CLOUD_SERVICES",
                COALESCE(mh."CREDITS_USED", 'N/A') as "CREDITS_USED",
                COALESCE(mh."BYTES", 'N/A') as "BYTES",
                COALESCE(mh."ROWS", 'N/A') as "ROWS",
                COALESCE(mh."FILES", 'N/A') as "FILES" 
        FROM metering_history mh
        FULL OUTER JOIN query_history qh ON QH."USER_NAME" = MH."NAME"
)

SELECT      
    {{dbt_utils.generate_surrogate_key(
         ['"NAME"']
     )}} AS "METERING_HISTORY_ID", *,
     current_timestamp as "{{var('col_create_dts')}}",
      current_timestamp as "{{var('col_update_dts')}}"
FROM dimension
