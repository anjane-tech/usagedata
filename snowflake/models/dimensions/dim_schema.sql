{{
  config(
      materialized='incremental',
      unique_key = '"SCHEMA_ID"',
      merge_update_columns = [var("col_update_dts"),'CATALOG_NAME','SCHEMA_NAME','IS_MANAGED_ACCESS','SCHEMA_OWNER','IS_TRANSIENT','SQL_PATH','CREATED','LAST_ALTERED'],
      tags = ["dimensions"]
  )  
}}

with schemas as (
    SELECT * 
    FROM {{ref('schema_stage_vw')}}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY "CATALOG_NAME","SCHEMA_NAME" ORDER BY LAST_ALTERED DESC) = 1
),

query_history as (
    select * 
    from {{ref('query_history_stage')}}
),

dimensions as(
    SELECT DISTINCT           
           CASE WHEN NULLIF(TRIM(s."CATALOG_NAME"),'') IS NOT NULL THEN S."CATALOG_NAME"
                WHEN NULLIF(TRIM(qh."DATABASE_NAME"),'') IS NOT NULL THEN QH."DATABASE_NAME"
                ELSE 'N/A' END AS "CATALOG_NAME",    
           CASE WHEN NULLIF(TRIM(s."SCHEMA_NAME"),'') IS NOT NULL THEN S."SCHEMA_NAME"
                WHEN NULLIF(TRIM(qh."SCHEMA_NAME"),'') IS NOT NULL THEN QH."SCHEMA_NAME"
                ELSE 'N/A' END AS "SCHEMA_NAME",
           COALESCE (s."SCHEMA_OWNER", 'N/A') AS "SCHEMA_OWNER",
           COALESCE (s."IS_TRANSIENT", 'N/A') AS "IS_TRANSIENT",
           COALESCE (s."IS_MANAGED_ACCESS", 'N/A') AS "IS_MANAGED_ACCESS",
           COALESCE (s."DEFAULT_CHARACTER_SET_CATALOG", 'N/A') AS "DEFAULT_CHARACTER_SET_CATALOG",
           COALESCE (s."DEFAULT_CHARACTER_SET_SCHEMA", 'N/A') AS "DEFAULT_CHARACTER_SET_SCHEMA",
           COALESCE (s."DEFAULT_CHARACTER_SET_NAME", 'N/A') AS "DEFAULT_CHARACTER_SET_NAME",
           COALESCE (s."SQL_PATH", 'N/A') AS "SQL_PATH",
           COALESCE (s."CREATED", to_timestamp_ntz('1901-01-01')) AS "CREATED",
           COALESCE (s."LAST_ALTERED", to_timestamp_ntz('1901-01-01')) AS "LAST_ALTERED",
           COALESCE (s."COMMENT", 'N/A') AS "COMMENT",
           COALESCE (s."DELETED", 'N/A') AS "DELETED"
    FROM query_history qh
    FULL OUTER JOIN schemas s on S."SCHEMA_NAME" = QH."SCHEMA_NAME"
)

SELECT 
     {{ dbt_utils.generate_surrogate_key(
         ['"CATALOG_NAME"','"SCHEMA_NAME"']
     )}} as "SCHEMA_ID", *,
     current_timestamp as "{{var('col_create_dts')}}",
        current_timestamp as "{{var('col_update_dts')}}"
FROM dimensions
