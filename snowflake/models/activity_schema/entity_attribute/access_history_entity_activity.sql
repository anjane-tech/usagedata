{{
  config(
    materialized='incremental',
    unique_key = '"ACCESS_HISTORY_ID"',
    merge_update_columns = [var("col_update_dts"),'BASE_OBJECTS_ACCESSED','USER_NAME','DIRECT_OBJECTS_ACCESSED','OBJECTS_MODIFIED'],
    tags = ["dimensions"],
    schema = var("usage_data_entityattribute_schema_name")
  )
}}

with access_history as (
    SELECT * 
    FROM {{ref('access_history_stage')}}
),

query_history as (
    SELECT * 
    FROM {{ref('query_history_stage')}}
),


dimension as (
    SELECT DISTINCT
           CASE WHEN NULLIF(TRIM(ah."QUERY_ID"),'') IS NOT NULL THEN AH."QUERY_ID"
                WHEN NULLIF(TRIM(qh."QUERY_ID"),'') IS NOT NULL THEN QH."QUERY_ID"
                ELSE 'N/A' END AS "QUERY_ID",
           COALESCE (ah."QUERY_START_TIME", '1901-01-01') AS "QUERY_START_TIME",
           CASE WHEN ah."USER_NAME" IS NOT NULL THEN AH."USER_NAME"
                WHEN qh."USER_NAME" IS NOT NULL THEN QH."USER_NAME"
                ELSE 'N/A' END AS "USER_NAME",
           ah."DIRECT_OBJECTS_ACCESSED" AS "DIRECT_OBJECTS_ACCESSED",
           ah."BASE_OBJECTS_ACCESSED" AS "BASE_OBJECTS_ACCESSED",
           ah."OBJECTS_MODIFIED" AS "OBJECTS_MODIFIED"
    FROM query_history qh
    INNER JOIN access_history ah ON AH."QUERY_ID" = QH."QUERY_ID"
)

SELECT
    'access_history' as entity_name,
    {{dbt_utils.generate_surrogate_key(
        ['"QUERY_ID"']
    )}} AS id,
    QUERY_ID as attribute_1,
    QUERY_START_TIME as attribute_2,
    USER_NAME as attribute_3,
    DIRECT_OBJECTS_ACCESSED as attribute_4,
    BASE_OBJECTS_ACCESSED as attribute_5,
    OBJECTS_MODIFIED as attribute_6,
    '' as attribute_7,
    '' as attribute_8,
    '' as attribute_9,
    '' as attribute_10,
    current_timestamp as "{{var('col_create_dts')}}",
    current_timestamp as "{{var('col_update_dts')}}"
FROM dimension    
