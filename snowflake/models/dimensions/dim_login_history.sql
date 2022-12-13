{{
  config(
    materialized='incremental',
    unique_key = '"LOGIN_HISTORY_ID"',
    merge_update_columns = ['var("col_update_dts")','LOGIN_EVENT_ID','EVENT_TYPE','CLIENT_IP','USER_NAME','REPORTED_CLIENT_TYPE','IS_SUCCESS','ERROR_CODE','RELATED_EVENT_ID','CONNECTION'],
    tags = ["dimensions"]
  )
}}

with login_history as (
    SELECT * 
    FROM {{ref('login_history_stage')}}
),

sessions as (
    SELECT * 
    FROM {{ref('sessions_stage')}}
),

dimension as(
     SELECT DISTINCT
           COALESCE (l."EVENT_TIMESTAMP", to_timestamp_ntz('1901-01-01')) AS "EVENT_TIMESTAMP",
           CASE WHEN l."EVENT_ID" IS NOT NULL THEN L."EVENT_ID"
                WHEN s."LOGIN_EVENT_ID" IS NOT NULL THEN S."LOGIN_EVENT_ID"
                ELSE 0 END AS "EVENT_ID",
           COALESCE (l."EVENT_TYPE", 'N/A') AS "EVENT_TYPE",
           CASE WHEN l."USER_NAME" IS NOT NULL THEN L."USER_NAME"
                WHEN s."USER_NAME" IS NOT NULL THEN S."USER_NAME"
                ELSE 'N/A' END AS "USER_NAME",
           COALESCE (l."CLIENT_IP", 'N/A') AS "CLIENT_IP",
           COALESCE (l."REPORTED_CLIENT_TYPE", 'N/A') AS "REPORTED_CLIENT_TYPE",
           COALESCE (l."REPORTED_CLIENT_VERSION", 0) AS "REPORTED_CLIENT_VERSION",
           COALESCE (l."FIRST_AUTHENTICATION_FACTOR", 'N/A') AS "FIRST_AUTHENTICATION_FACTOR",
           COALESCE (l."SECOND_AUTHENTICATION_FACTOR", 'N/A') AS "SECOND_AUTHENTICATION_FACTOR",
           COALESCE (l."IS_SUCCESS", 'N/A') AS "IS_SUCCESS",
           COALESCE (l."ERROR_CODE", 'N/A') AS "ERROR_CODE",
           COALESCE (l."ERROR_MESSAGE", 'N/A') AS "ERROR_MESSAGE",
           COALESCE (l."RELATED_EVENT_ID", 'N/A') AS "RELATED_EVENT_ID",
           COALESCE (l."CONNECTION", 'N/A') AS "CONNECTION"
    FROM sessions s
    FULL OUTER JOIN login_history l ON L."EVENT_ID" = S."LOGIN_EVENT_ID"
)

SELECT 
     {{ dbt_utils.generate_surrogate_key(
         ['"EVENT_ID"']
     )}} as "LOGIN_HISTORY_ID", *,
     current_timestamp as "{{var('col_create_dts')}}",
      current_timestamp as "{{var('col_update_dts')}}"
FROM dimension
