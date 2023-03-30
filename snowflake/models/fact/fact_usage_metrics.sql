
{{
  config(
    materialized='incremental',
    merge_update_columns = ['var("col_update_dts")','ACCESS_HISTORY_ID','DATABASE_ID','ERROR_ID','LOGIN_HISTORY_ID','ROLES_ID','SCHEMA_ID','COMBINED_SESSION_ID','TABLE_ID','TAGS_ID','COMBINED_USERS_ID','COMBINED_WAREHOUSE_ID','QUERY_ID'],
    tags = ["fact"],
    schema = var("usage_data_datamart_schema_name")
  )
}}

  WITH query_history AS (
    SELECT * FROM {{ref("query_history_stage")}}
  ),

    total_table_count as (
    select distinct "QUERY_ID",
    count (distinct "table_name") as "TABLE_COUNT" 
    from {{ref("access_history_vw")}}
    group by 1
  ),
  Access_history as (
         select "QUERY_ID",
         COALESCE(NULLIF(TRIM("database_name"),''), 'N/A') "AH_database_name",
         COALESCE(NULLIF(TRIM("schema_name"),''), 'N/A') "AH_schema_name",
         COALESCE(NULLIF(TRIM("table_name"),''), 'N/A') "AH_table_name"
         from {{ref("access_history_vw")}})

SELECT 
        AH."QUERY_ID" as "ACCESS_HISTORY_ID",

        LH."LOGIN_HISTORY_ID",
        SE."COMBINED_SESSION_ID",

         {{dbt_utils.generate_surrogate_key(["QH.QUERY_ID"])}} "QUERY_ID",
         {{dbt_utils.generate_surrogate_key(["QH.DATABASE_NAME"])}} "DATABASE_ID",
         {{dbt_utils.generate_surrogate_key(["QH.ERROR_CODE"])}} "ERROR_ID",
         {{dbt_utils.generate_surrogate_key(["QH.ROLE_NAME"])}} "ROLES_ID",
         {{dbt_utils.generate_surrogate_key(["QH.DATABASE_NAME","SCHEMA_NAME"])}} "SCHEMA_ID",
         {{dbt_utils.generate_surrogate_key(["QH.QUERY_TAG","DATABASE_NAME","SCHEMA_NAME"])}} "TAGS_ID",
         {{dbt_utils.generate_surrogate_key(["QH.USER_NAME"])}} "COMBINED_USERS_ID",
         {{dbt_utils.generate_surrogate_key(["QH.WAREHOUSE_NAME"])}} "COMBINED_WAREHOUSE_ID", 
         {{dbt_utils.generate_surrogate_key(['"AH_database_name"','"AH_table_name"','"AH_schema_name"'])}} "TABLE_ID",     


        to_char(QH."START_TIME", 'YYYYMMDD') as "QUERY_START_DT",
        to_char(QH."END_TIME", 'YYYYMMDD') as "QUERY_END_DT",
        SUM(timestampdiff(milliseconds, QH."START_TIME", QH."END_TIME")/1000)/TC."TABLE_COUNT" AS "QUERY_TIME_IN_SECS",
        SUM(QH."BYTES_SCANNED"::DECIMAL(38, 0))/TC."TABLE_COUNT" AS "BYTES_SCANNED",
        SUM(QH."OUTBOUND_DATA_TRANSFER_BYTES"::DECIMAL(38, 0))/TC."TABLE_COUNT" AS "OUTBOUND_DATA_TRANSFER_BYTES",
        SUM(QH."INBOUND_DATA_TRANSFER_BYTES"::DECIMAL(38, 0))/TC."TABLE_COUNT" AS "INBOUND_DATA_TRANSFER_BYTES",
        SUM(QH."EXTERNAL_FUNCTION_TOTAL_SENT_BYTES"::DECIMAL(38, 0))/TC."TABLE_COUNT" AS "EXTERNAL_FUNCTION_TOTAL_SENT_BYTES",
        SUM(CPQ."COMPUTE_COST"::DECIMAL(38, 5))/TC."TABLE_COUNT" AS "COMPUTE_COST",
        SUM(CPQ."CLOUD_SERVICES_COST"::DECIMAL(38, 5))/TC."TABLE_COUNT" AS "CLOUD_SERVICES_COST",
        SUM(CPQ."QUERY_COST"::DECIMAL(38, 5))/TC."TABLE_COUNT" AS "QUERY_COST",
        CPQ.CURRENCY,
        current_timestamp as "{{var('col_create_dts')}}",
        current_timestamp as "{{var('col_update_dts')}}"

    FROM query_history QH 

      INNER JOIN total_table_count TC ON TC."QUERY_ID" = QH."QUERY_ID"
      INNER JOIN Access_history AH ON AH."QUERY_ID" = COALESCE(NULLIF(TRIM(QH."QUERY_ID"),''), 'N/A')
      INNER JOIN {{ref("dim_sessions")}} SE ON SE."SESSION_ID" = COALESCE(QH."SESSION_ID", 0)
      INNER JOIN {{ref("dim_login_history")}} LH ON LH."EVENT_ID" = SE."LOGIN_EVENT_ID"
      INNER JOIN {{ref("cost_per_query")}} CPQ ON CPQ."QUERY_ID" = QH."QUERY_ID"

GROUP BY
        AH."QUERY_ID",
        QH."QUERY_ID",
        QH."DATABASE_NAME",
        QH."ERROR_CODE",
        QH."ROLE_NAME",
        QH."SCHEMA_NAME",
        QH."QUERY_TAG",
        QH."USER_NAME",
        QH."WAREHOUSE_NAME",
        "AH_database_name",
        "AH_table_name",
        "AH_schema_name",
        LH."LOGIN_HISTORY_ID",
        SE."COMBINED_SESSION_ID",        
        CPQ."CURRENCY",
        TC."TABLE_COUNT",
        to_char(QH."START_TIME", 'YYYYMMDD'),
        to_char(QH."END_TIME", 'YYYYMMDD')


 