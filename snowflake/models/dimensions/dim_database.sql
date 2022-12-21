{{
  config(
    materialized='incremental',
    unique_key = '"DATABASE_ID"',
    merge_update_columns = [var("col_update_dts"),'DATABASE_NAME','DATABASE_OWNER','IS_TRANSIENT','COMMENT','LAST_ALTERED'],
    tags = ["dimensions"]
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
    {{dbt_utils.generate_surrogate_key(
         ['"DATABASE_NAME"']
     )}} AS "DATABASE_ID", *,
     current_timestamp as "{{var('col_create_dts')}}",
      current_timestamp as "{{var('col_update_dts')}}"
FROM dimension
