{{
  config(
    materialized='incremental',
    unique_key = '"ACCESS_HISTORY_ID"',
    merge_update_columns = ['var("col_update_dts")','BASE_OBJECTS_ACCESSED','USER_NAME','DIRECT_OBJECTS_ACCESSED','OBJECTS_MODIFIED'],
    tags = ["dimensions"]
  )
}}

with access_history as (
    SELECT * 
    FROM {{ref('access_history_stage')}}
    {% if is_incremental() %}
    WHERE "QUERY_START_TIME" > (SELECT max("{{var('col_update_dts')}}") from {{this}})
    {% endif %}
),

query_history as (
    SELECT * 
    FROM {{ref('query_history_stage')}}
    {% if is_incremental() %}
    WHERE "END_TIME" > (SELECT max("{{var('col_update_dts')}}") FROM {{this}})
    {% endif %}
),


dimension as (
    SELECT DISTINCT
            current_timestamp as "CREATEDTS",
            current_timestamp as "UPDATEDDTS",
           CASE WHEN ah."QUERY_ID" IS NOT NULL THEN AH."QUERY_ID"
                WHEN qh."QUERY_ID" IS NOT NULL THEN QH."QUERY_ID"
                ELSE 'N/A' END AS "QUERY_ID",
           COALESCE (ah."QUERY_START_TIME", '1901-01-01') AS "QUERY_START_TIME",
           CASE WHEN ah."USER_NAME" IS NOT NULL THEN AH."USER_NAME"
                WHEN qh."USER_NAME" IS NOT NULL THEN QH."USER_NAME"
                ELSE 'N/A' END AS "USER_NAME",
           COALESCE (ah."DIRECT_OBJECTS_ACCESSED", 'N/A') AS "DIRECT_OBJECTS_ACCESSED",
           COALESCE (ah."BASE_OBJECTS_ACCESSED", 'N/A') AS "BASE_OBJECTS_ACCESSED",
           COALESCE (ah."OBJECTS_MODIFIED", 'N/A') AS "OBJECTS_MODIFIED"
    FROM query_history qh
    FULL OUTER JOIN access_history ah ON AH."QUERY_ID" = QH."QUERY_ID"
)

SELECT
    {{dbt_utils.surrogate_key(
        ['"QUERY_ID"']
    )}} AS "ACCESS_HISTORY_ID", *
FROM dimension    
