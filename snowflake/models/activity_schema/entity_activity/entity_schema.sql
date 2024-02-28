{{
  config(
      materialized='incremental',
      unique_key = '"SCHEMA_ID"',
      merge_update_columns = [var("col_update_dts"),'CATALOG_NAME','SCHEMA_NAME','IS_MANAGED_ACCESS','SCHEMA_OWNER','IS_TRANSIENT','SQL_PATH','CREATED','LAST_ALTERED'],
      tags = ["dimensions"],
      schema = var("usage_data_datamart_schema_name")
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
           COALESCE (s."DELETED", to_timestamp_ntz('1901-01-01')) AS "DELETED"
    FROM query_history qh
    FULL OUTER JOIN schemas s on S."SCHEMA_NAME" = QH."SCHEMA_NAME"
)

SELECT 
    'schema' as entity_name,
    {{ dbt_utils.generate_surrogate_key(
        ['"CATALOG_NAME"','"SCHEMA_NAME"']
    )}} as id,
    CATALOG_NAME as attribute_1,
    SCHEMA_NAME as attribute_2,
    SCHEMA_OWNER as attribute_3,
    IS_TRANSIENT as attribute_4,
    IS_MANAGED_ACCESS as attribute_5,
    DEFAULT_CHARACTER_SET_CATALOG as attribute_6,
    DEFAULT_CHARACTER_SET_SCHEMA as attribute_7,
    DEFAULT_CHARACTER_SET_NAME as attribute_8,
    SQL_PATH as attribute_9,
    CREATED as attribute_10,
    -- LAST_ALTERED as attribute_11,
    -- COMMENT as attribute_12,
    -- DELETED as attribute_13,
    current_timestamp as "{{var('col_create_dts')}}",
    current_timestamp as "{{var('col_update_dts')}}"

FROM dimensions
