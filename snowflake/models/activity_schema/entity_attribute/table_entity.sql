{{
  config(
    materialized='incremental',
    unique_key = '"TABLE_ID"',
    merge_update_columns = [var("col_update_dts"),'TABLE_CATALOG','TABLE_NAME','TABLE_OWNER','ROW_COUNT'],
    tags = ["dimensions"],
    schema = var("usage_data_entityattribute_schema_name")
  )
}}

with stage_table as (
    SELECT * 
    FROM {{ref('table_stage_vw')}}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY "TABLE_CATALOG","TABLE_SCHEMA","TABLE_NAME" ORDER BY LAST_ALTERED DESC) = 1
),

dimension as(
    SELECT DISTINCT
           CASE WHEN NULLIF(TRIM(ah."database_name"),'') IS NOT NULL THEN ah."database_name"
                WHEN NULLIF(TRIM(t."TABLE_CATALOG"),'') IS NOT NULL THEN T."TABLE_CATALOG"
                ELSE 'N/A' END AS "TABLE_CATALOG",
           CASE WHEN NULLIF(TRIM(ah."schema_name"),'') IS NOT NULL THEN ah."schema_name"
                WHEN NULLIF(TRIM(t."TABLE_SCHEMA"),'') IS NOT NULL THEN T."TABLE_SCHEMA"
                ELSE  'N/A' END AS "TABLE_SCHEMA",
           CASE WHEN NULLIF(TRIM(t."TABLE_NAME"),'') IS NOT NULL THEN T."TABLE_NAME"
                WHEN NULLIF(TRIM(ah."table_name"),'') IS NOT NULL THEN AH."table_name"
                ELSE 'N/A' END AS "TABLE_NAME",
           COALESCE (t."TABLE_OWNER", 'N/A') AS "TABLE_OWNER",
           COALESCE (t."TABLE_TYPE", 'N/A') AS "TABLE_TYPE",
           COALESCE (t."ROW_COUNT", 0) AS "ROW_COUNT",
           COALESCE (t."CREATED", to_timestamp_ntz('1901-01-01')) AS "CREATED",
           COALESCE (t."DELETED", to_timestamp_ntz('1901-01-01')) AS "DELETED"
    FROM {{ref('access_history_vw')}} ah
    FULL OUTER JOIN stage_table t ON COALESCE (NULLIF(TRIM(t."TABLE_CATALOG"),''),'N/A') = COALESCE (NULLIF(TRIM(ah."database_name"),''),'N/A')  
                                    and COALESCE (NULLIF(TRIM(t."TABLE_SCHEMA"),''),'N/A') = COALESCE (NULLIF(TRIM(ah."schema_name"),''),'N/A')
                                    and COALESCE (NULLIF(TRIM(t."TABLE_NAME"),''),'N/A') = COALESCE (NULLIF(TRIM(ah."table_name"),''),'N/A')
)
SELECT
     'table' as entity_name,
    {{dbt_utils.generate_surrogate_key(
        ['"TABLE_CATALOG"','"TABLE_SCHEMA"','"TABLE_NAME"']
    )}} AS id, 
    TABLE_CATALOG as attribute_1,
    TABLE_SCHEMA as attribute_2,
    TABLE_NAME as attribute_3,
    TABLE_OWNER as attribute_4,
    TABLE_TYPE as attribute_5,
    ROW_COUNT as attribute_6,
    CREATED as attribute_7,
    DELETED as attribute_8,
    '' as attribute_9,
    '' as attribute_10,
    current_timestamp as "{{var('col_create_dts')}}",
      current_timestamp as "{{var('col_update_dts')}}"
FROM dimension
