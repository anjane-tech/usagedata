{{
  config(
    materialized='incremental',
    unique_key = '"TABLE_ID"',
    merge_update_columns = ['var("col_update_dts")','TABLE_CATALOG','TABLE_NAME','TABLE_OWNER','ROW_COUNT'],
    tags = ["dimensions"]
  )
}}

with stage_table as (
    SELECT * 
    FROM {{ref('table_stage')}}
    {% if is_incremental() %}
    where "LAST_ALTERED" > (select max("{{var('col_update_dts')}}") from {{this}})
    {% endif %}
),

dimension as(
    SELECT DISTINCT
            current_timestamp as "{{var('col_create_dts')}}",
            current_timestamp as "{{var('col_update_dts')}}",
           CASE WHEN ah."database_name" IS NOT NULL THEN ah."database_name"
                WHEN  t."TABLE_CATALOG" IS NOT NULL THEN T."TABLE_CATALOG"
                ELSE 'N/A' END AS "TABLE_CATALOG",
           CASE WHEN ah."schema_name" IS NOT NULL THEN ah."schema_name"
                WHEN t."TABLE_NAME" IS NOT NULL THEN T."TABLE_NAME"
                ELSE  'N/A' END AS "TABLE_SCHEMA",
           CASE WHEN t."TABLE_NAME" IS NOT NULL THEN T."TABLE_NAME"
                WHEN ah."table_name" IS NOT NULL THEN AH."table_name"
                ELSE 'N/A' END AS "TABLE_NAME",
           COALESCE (t."TABLE_OWNER", 'N/A') AS "TABLE_OWNER",
           COALESCE (t."TABLE_TYPE", 'N/A') AS "TABLE_TYPE",
           COALESCE (t."ROW_COUNT", 'N/A') AS "ROW_COUNT",
           COALESCE (t."CREATED", 'N/A') AS "CREATED"
    FROM {{ref('access_history_temp')}} ah
    FULL OUTER JOIN stage_table t ON T."TABLE_CATALOG" = ah."database_name" 
                                    and T."TABLE_SCHEMA" = ah."schema_name"
                                    and T."TABLE_NAME" = ah."table_name"
)

SELECT
    {{dbt_utils.surrogate_key(
        ['"TABLE_CATALOG"','"TABLE_SCHEMA"','"TABLE_NAME"']
    )}} AS "TABLE_ID", *
FROM dimension
