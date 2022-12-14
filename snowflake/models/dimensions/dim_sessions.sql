{{
  config(
    materialized='incremental',
    unique_key = '"COMBINED_SESSION_ID"',
    merge_update_columns = ['var("col_update_dts")','USER_NAME','AUTHENTICATION_METHOD','CLIENT_APPLICATION_VERSION','AUTHENTICATION_METHOD','LOGIN_EVENT_ID','CLIENT_VERSION'],
    tags = ["dimensions"]
  )
}}

with session_tmp as (
    SELECT * 
    FROM {{ref('sessions_stage')}}
),

query_history as (
    select * 
    from {{ref('query_history_stage')}}
),

dimension as(
    SELECT DISTINCT
           CASE WHEN s."SESSION_ID" IS NOT NULL THEN S."SESSION_ID"
                WHEN qh."SESSION_ID" IS NOT NULL THEN QH."SESSION_ID"
                ELSE 0 END AS "SESSION_ID",
           CASE WHEN s."USER_NAME" IS NOT NULL THEN S."USER_NAME"
                WHEN qh."USER_NAME" IS NOT NULL THEN QH."USER_NAME"
                ELSE 'N/A' END AS "USER_NAME",
           COALESCE(s."CREATED_ON", to_timestamp_ntz('1901-01-01')) AS "CREATED_ON",
           COALESCE(s."AUTHENTICATION_METHOD", 'N/A') AS "AUTHENTICATION_METHOD",
           COALESCE(s."LOGIN_EVENT_ID", 0) AS "LOGIN_EVENT_ID",
           COALESCE(s."CLIENT_APPLICATION_VERSION", 'N/A') AS "CLIENT_APPLICATION_VERSION",
           COALESCE(s."CLIENT_APPLICATION_ID", 'N/A') AS "CLIENT_APPLICATION_ID",
           COALESCE(s."CLIENT_ENVIRONMENT", 'N/A') AS "CLIENT_ENVIRONMENT",
           COALESCE(s."CLIENT_BUILD_ID", 0) AS "CLIENT_BUILD_ID",
           COALESCE(s."CLIENT_VERSION", 0) AS "CLIENT_VERSION"
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