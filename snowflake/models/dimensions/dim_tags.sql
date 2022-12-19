{{
  config(
    materialized='incremental',
    unique_key = '"TAGS_ID"',
    merge_update_columns = [var("col_update_dts"),'TAG_NAME','TAG_SCHEMA','TAG_COMMENT','TAG_OWNER','CREATED','LAST_ALTERED'],
    tags = ["dimensions"]
  )
}}

with tags as (
    SELECT * 
    FROM {{ref('tags_stage_vw')}}
),

query_history as (
    select * 
    from {{ref('query_history_stage')}}
),

dimension as(
    SELECT DISTINCT
           CASE WHEN t."TAG_NAME" IS NOT NULL THEN T."TAG_NAME"
                WHEN qh."QUERY_TAG" IS NOT NULL THEN QH."QUERY_TAG"
                ELSE 'N/A' END AS "TAG_NAME",
           CASE WHEN t."TAG_SCHEMA" IS NOT NULL THEN T."TAG_SCHEMA"
                WHEN qh."SCHEMA_NAME" IS NOT NULL THEN QH."SCHEMA_NAME"
                ELSE 'N/A' END AS "TAG_SCHEMA", 
           CASE WHEN t."TAG_DATABASE" IS NOT NULL THEN T."TAG_DATABASE"
                WHEN qh."DATABASE_NAME" IS NOT NULL THEN QH."DATABASE_NAME"
                ELSE 'N/A' END AS "TAG_DATABASE",
           COALESCE (t."TAG_OWNER", 'N/A') AS "TAG_OWNER",
           COALESCE (t."TAG_COMMENT", 'N/A') AS "TAG_COMMENT",
           COALESCE (t."CREATED", to_timestamp_ntz('1901-01-01')) AS "CREATED",
           COALESCE (t."LAST_ALTERED", to_timestamp_ntz('1901-01-01')) AS "LAST_ALTERED",
           COALESCE (t."DELETED", to_timestamp_ntz('1901-01-01')) AS "DELETED",
           COALESCE (t."ALLOWED_VALUES", 'N/A') AS "ALLOWED_VALUES"
    FROM query_history qh
    FULL OUTER JOIN tags t ON T."TAG_NAME" = QH."QUERY_TAG"
)

SELECT 
     {{ dbt_utils.generate_surrogate_key(
         ['"TAG_NAME"','"TAG_DATABASE"','"TAG_SCHEMA"']
     )}} as "TAGS_ID", *,
     current_timestamp as "{{var('col_create_dts')}}",
      current_timestamp as "{{var('col_update_dts')}}"
FROM dimension

