{{
  config(
    materialized='incremental',
    unique_key = '"LOGIN_HISTORY_ID"',
    merge_update_columns = [var("col_update_dts"),'EVENT_ID','EVENT_TYPE','CLIENT_IP','USER_NAME','REPORTED_CLIENT_TYPE','IS_SUCCESS','ERROR_CODE','RELATED_EVENT_ID','CONNECTION'],
    tags = ["dimensions"],
    schema = var("usage_data_datamart_schema_name")
  )
}}

with login_history as (
    SELECT * 
    FROM {{ref('login_history_stage_vw')}}
),

sessions as (
    SELECT * 
    FROM {{ref('sessions_stage_vw')}}
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
           COALESCE (l."REPORTED_CLIENT_VERSION", 'N/A') AS "REPORTED_CLIENT_VERSION",
           COALESCE (l."FIRST_AUTHENTICATION_FACTOR", 'N/A') AS "FIRST_AUTHENTICATION_FACTOR",
           COALESCE (l."SECOND_AUTHENTICATION_FACTOR", 'N/A') AS "SECOND_AUTHENTICATION_FACTOR",
           COALESCE (l."IS_SUCCESS", 'N/A') AS "IS_SUCCESS",
           COALESCE (l."ERROR_CODE", 0) AS "ERROR_CODE",
           COALESCE (l."ERROR_MESSAGE", 'N/A') AS "ERROR_MESSAGE",
           l."RELATED_EVENT_ID" AS "RELATED_EVENT_ID",
           COALESCE (l."CONNECTION", 'N/A') AS "CONNECTION"
    FROM sessions s
    FULL OUTER JOIN login_history l ON L."EVENT_ID" = S."LOGIN_EVENT_ID"
)

SELECT 
    'login_history' as entity_name,
    {{ dbt_utils.generate_surrogate_key(
         ['"EVENT_ID"']
    )}} as id,
    'EVENT_TIMESTAMP' as attribute_1,
    'EVENT_ID' as attribute_2,
    'EVENT_TYPE' as attribute_3,
    'USER_NAME' as attribute_4,
    'CLIENT_IP' as attribute_5,
    'REPORTED_CLIENT_TYPE' as attribute_6,
    'REPORTED_CLIENT_VERSION' as attribute_7,
    'FIRST_AUTHENTICATION_FACTOR' as attribute_8,
    'SECOND_AUTHENTICATION_FACTOR' as attribute_9,
    'IS_SUCCESS' as attribute_10,
    'ERROR_CODE' as attribute_11,
    'ERROR_MESSAGE' as attribute_12,
    'RELATED_EVENT_ID' as attribute_13,
    'CONNECTION' as attribute_14,
    current_timestamp as "{{var('col_create_dts')}}",
    current_timestamp as "{{var('col_update_dts')}}"
FROM dimension
