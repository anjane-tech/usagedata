{{
  config(
    materialized='incremental',
    unique_key = '"METERING_HISTORY_ID"',
    merge_update_columns = [var("col_update_dts"),],
    tags = ["dimensions"],
    schema = var("usage_data_datamart_schema_name")
  )
}}

with replication_usage_history as (
    select * from 
    {{ref('replication_usage_history_stage_vw')}} 
),

database_tb as (
    select * FROM
    {{ref('database_stage_vw')}}
),

dimension as (
    SELECT DISTINCT
                COALESCE(ru."ORGANIZATION_NAME", 'N/A') as "ORGANIZATION_NAME",
                COALESCE(ru."ACCOUNT_NAME", 'N/A') AS "ACCOUNT_NAME",
                COALESCE(ru."ACCOUNT_LOCATOR",'N/A') AS 'ACCOUNT_LOCATOR',
                COALESCE(ru."REGION",'N/A') AS 'REGION',
                COALESCE(ru."USAGE_DATE",'N/A') AS 'USAGE_DATE',
                COALESCE(ru."CREDITS_USED",'N/A') AS 'CREDITS_USED',
                COALESCE(ru."BYTES_TRANSFERED",'N/A') AS 'BYTES_TRANSFERED',
                CASE WHEN ru."DATABASE_ID" IS NULL THEN RU."DATABASE_ID"
                     THEN d."DATABASE_ID" IS NULL THEN D."DATABASE_ID"
                     ELSE 'N/A' END AS "DATABASE_ID",
                CASE WHEN ru."DATABASE_NAME" IS NULL THEN RU."DATABASE_NAME"
                     THEN d."DATABASE_NAME" IS NULL THEN D."DATABASE_NAME"
                     ELSE 'N/A' END AS "DATABASE_NAME"
        FROM replication_usage_history ru
        FULL OUTER JOIN database_tb d on d."DATABASE_ID" = ru."DATABASE_ID"
)

SELECT 
     {{ dbt_utils.generate_surrogate_key(
         ['"DATABASE_ID"']
     )}} as "REPLICATION_USAGE_HISTORY_ID", *,
     current_timestamp as "{{var('col_create_dts')}}",
      current_timestamp as "{{var('col_update_dts')}}"
FROM dimension