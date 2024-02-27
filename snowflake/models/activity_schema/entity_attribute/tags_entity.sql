{{
  config(
    materialized='incremental',
    unique_key = '"TAGS_ID"',
    merge_update_columns = [var("col_update_dts"),'TAG_NAME','TAG_SCHEMA','TAG_COMMENT','TAG_OWNER','CREATED','LAST_ALTERED'],
    tags = ["dimensions"],
    schema = var("usage_data_entityattribute_schema_name")
  )
}}

with tags as (
    SELECT * 
    FROM {{ref('tags_stage_vw')}}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY "TAG_NAME","TAG_DATABASE","TAG_SCHEMA" ORDER BY LAST_ALTERED DESC) = 1
),

query_history as (
    select * 
    from {{ref('query_history_stage')}}
),

dimension as(
    SELECT DISTINCT
           CASE WHEN NULLIF(TRIM(t."TAG_NAME"),'') IS NOT NULL THEN T."TAG_NAME"
                WHEN NULLIF(TRIM(qh."QUERY_TAG"),'') IS NOT NULL THEN QH."QUERY_TAG"
                ELSE 'N/A' END AS "TAG_NAME",
           CASE WHEN NULLIF(TRIM(t."TAG_SCHEMA"),'') IS NOT NULL THEN T."TAG_SCHEMA" ---
                WHEN NULLIF(TRIM(qh."SCHEMA_NAME"),'') IS NOT NULL THEN QH."SCHEMA_NAME"
                ELSE 'N/A' END AS "TAG_SCHEMA", 
           CASE WHEN NULLIF(TRIM(t."TAG_DATABASE"),'') IS NOT NULL THEN T."TAG_DATABASE"
                WHEN NULLIF(TRIM(qh."DATABASE_NAME"),'') IS NOT NULL THEN QH."DATABASE_NAME"
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
     'tags' as enity_name,
     {{ dbt_utils.generate_surrogate_key(
         ['"TAG_NAME"','"TAG_DATABASE"','"TAG_SCHEMA"']
     )}} as id, 
     TAG_NAME as attribute_1,
     TAG_SCHEMA as atrribute_2,
     TAG_DATABASE as attribute_3,
     TAG_OWNER as attribute_4,
     TAG_COMMENT as attribute_5,
     CREATED as attribute_6,
     LAST_ALTERED as attribute_7,
     DELETED as attribute_8,
     ALLOWED_VALUES as attribute_9,
     '' as attribute_10,
     current_timestamp as "{{var('col_create_dts')}}",
      current_timestamp as "{{var('col_update_dts')}}"
FROM dimension

