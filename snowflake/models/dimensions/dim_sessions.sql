{{
  config(
    materialized='incremental',
    unique_key = '"COMBINED_SESSION_ID"',
    merge_update_columns = [var("col_update_dts"),'USER_NAME','AUTHENTICATION_METHOD','CLIENT_APPLICATION_VERSION','LOGIN_EVENT_ID','CLIENT_VERSION'],
    tags = ["dimensions"]
  )
}}

with session_tmp as (
    SELECT * 
    FROM {{ref('sessions_stage_vw')}}
),

query_history as (
    select * 
    from {{ref('query_history_stage')}}
),

dimension as(
    SELECT DISTINCT
           CASE WHEN NULLIF(TRIM(s."SESSION_ID"),'') IS NOT NULL THEN S."SESSION_ID"
                WHEN NULLIF(TRIM(qh."SESSION_ID"),'') IS NOT NULL THEN QH."SESSION_ID"
                ELSE 'N/A' END AS "SESSION_ID",
           CASE WHEN s."USER_NAME" IS NOT NULL THEN S."USER_NAME"
                WHEN qh."USER_NAME" IS NOT NULL THEN QH."USER_NAME"
                ELSE 'N/A' END AS "USER_NAME",
           COALESCE(s."CREATED_ON", to_timestamp_ntz('1901-01-01')) AS "CREATED_ON",
           COALESCE(s."AUTHENTICATION_METHOD", 'N/A') AS "AUTHENTICATION_METHOD",
           s."LOGIN_EVENT_ID" AS "LOGIN_EVENT_ID",
           s."CLIENT_APPLICATION_VERSION" AS "CLIENT_APPLICATION_VERSION",
           s."CLIENT_APPLICATION_ID" AS "CLIENT_APPLICATION_ID",
           s."CLIENT_ENVIRONMENT" AS "CLIENT_ENVIRONMENT",
           s."CLIENT_BUILD_ID" AS "CLIENT_BUILD_ID",
           s."CLIENT_VERSION" AS "CLIENT_VERSION"
    FROM query_history qh
    FULL OUTER JOIN session_tmp s on S."SESSION_ID" = QH."SESSION_ID"
)

SELECT      
    {{dbt_utils.generate_surrogate_key(
         ['"SESSION_ID"']
     )}} AS "COMBINED_SESSION_ID", *,
     current_timestamp as "{{var('col_create_dts')}}",
      current_timestamp as "{{var('col_update_dts')}}"
      
FROM dimension