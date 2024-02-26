{{
  config(
    materialized='incremental',
    unique_key = '"DATABASE_ID"',
    merge_update_columns = [var("col_update_dts"),'DATABASE_NAME','DATABASE_OWNER','IS_TRANSIENT','COMMENT','LAST_ALTERED'],
    tags = ["dimensions"],
    schema = var("usage_data_datamart_schema_name")
  )
}}

with db as (
    SELECT * 
    FROM {{ref('database_stage_vw')}}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY DATABASE_NAME ORDER BY LAST_ALTERED DESC) = 1
),

query_history as (
    SELECT * 
    FROM {{ref('query_history_stage')}}
),

dimension as (
    SELECT DISTINCT
           CASE WHEN NULLIF(TRIM(d."DATABASE_NAME"), '') IS NOT NULL THEN TRIM(D."DATABASE_NAME")
                WHEN NULLIF(TRIM(qh."DATABASE_NAME"), '') IS NOT NULL THEN TRIM(QH."DATABASE_NAME")
                ELSE 'N/A' END AS "DATABASE_NAME",
           COALESCE (d."DATABASE_OWNER", 'N/A') AS "DATABASE_OWNER",
           COALESCE (d."IS_TRANSIENT", 'N/A') AS "IS_TRANSIENT",
           COALESCE (d."COMMENT", 'N/A') AS "COMMENT",
           COALESCE (d."CREATED", to_timestamp_ntz('1901-01-01')) AS "CREATED",
           COALESCE (d."LAST_ALTERED", to_timestamp_ntz('1901-01-01')) AS "LAST_ALTERED",
           COALESCE (d."DELETED", to_timestamp_ntz('1901-01-01')) AS "DELETED"
    FROM query_history qh
    FULL OUTER JOIN db d ON D."DATABASE_NAME" = QH."DATABASE_NAME"
)

SELECT     
    'database' as entity_name,
    {{dbt_utils.generate_surrogate_key(
         ['"DATABASE_NAME"']
     )}} AS id, 
    DATABASE_NAME as  attribute_1,
    DATABASE_OWNER as attribute_2,
    IS_TRANSIENT as attribute_3,
    COMMENT as attribute_4,
    CREATED as attribute_5,
    LAST_ALTERED as attribute_6,
    DELETED as attribute_7,
    '' as attribute_8,
    '' as attribute_9,
    '' as attribute_10,
    current_timestamp as "{{var('col_create_dts')}}",
    current_timestamp as "{{var('col_update_dts')}}"
FROM dimension
