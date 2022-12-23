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
  )

SELECT 
        AH."ACCESS_HISTORY_ID",
        D."DATABASE_ID",
        E."ERROR_ID",
        LH."LOGIN_HISTORY_ID",
        R."ROLES_ID",
        S."SCHEMA_ID",
        SE."COMBINED_SESSION_ID",
        T."TABLE_ID",
        TG."TAGS_ID",
        U."COMBINED_USERS_ID",
        W."COMBINED_WAREHOUSE_ID",
        QH."QUERY_ID",
        to_char(QH."START_TIME", 'YYYYMMDD') as "QUERY_START_DT",
        to_char(QH."END_TIME", 'YYYYMMDD') as "QUERY_END_DT",
        SUM(timestampdiff(milliseconds, QH."START_TIME", QH."END_TIME")/1000) AS "QUERY_TIME_IN_SECS",
        SUM(QH."BYTES_SCANNED"::DECIMAL(38, 0)) AS "BYTES_SCANNED",
        SUM(QH."OUTBOUND_DATA_TRANSFER_BYTES"::DECIMAL(38, 0)) AS "OUTBOUND_DATA_TRANSFER_BYTES",
        SUM(QH."INBOUND_DATA_TRANSFER_BYTES"::DECIMAL(38, 0)) AS "INBOUND_DATA_TRANSFER_BYTES",
        SUM(QH."EXTERNAL_FUNCTION_TOTAL_SENT_BYTES"::DECIMAL(38, 0)) AS "EXTERNAL_FUNCTION_TOTAL_SENT_BYTES",
        current_timestamp as "{{var('col_create_dts')}}",
        current_timestamp as "{{var('col_update_dts')}}"
    FROM query_history QH 
    INNER JOIN {{ref("dim_access_history")}} AH ON AH."QUERY_ID" = COALESCE(NULLIF(TRIM(QH."QUERY_ID"),''), 'N/A')
    INNER JOIN {{ref("dim_database")}} D ON D."DATABASE_NAME" = COALESCE(NULLIF(TRIM(QH."DATABASE_NAME"),''), 'N/A')
    INNER JOIN {{ref("dim_error")}} E ON E."ERROR_CODE" = COALESCE(QH."ERROR_CODE", 0)
    INNER JOIN {{ref("dim_sessions")}} SE ON SE."SESSION_ID" = COALESCE(QH."SESSION_ID", 0)
    INNER JOIN {{ref("dim_login_history")}} LH ON LH."EVENT_ID" = SE."LOGIN_EVENT_ID"
    INNER JOIN {{ref("dim_roles")}} R ON R."NAME" = COALESCE(NULLIF(TRIM(QH."ROLE_NAME"),''), 'N/A')
    INNER JOIN {{ref("dim_schema")}} S ON S."CATALOG_NAME" = COALESCE(NULLIF(TRIM(QH."DATABASE_NAME"),''), 'N/A') AND
                                           S."SCHEMA_NAME" = COALESCE(NULLIF(TRIM(QH."SCHEMA_NAME"),''), 'N/A')
    INNER JOIN {{ref("dim_tags")}} TG ON TG."TAG_NAME" = COALESCE(NULLIF(TRIM(QH."QUERY_TAG"),''), 'N/A') AND
                                       TG."TAG_SCHEMA" = COALESCE(NULLIF(TRIM(QH."SCHEMA_NAME"),''), 'N/A') AND
                                     TG."TAG_DATABASE" = COALESCE(NULLIF(TRIM(QH."DATABASE_NAME"),''), 'N/A')
    INNER JOIN {{ref("dim_users")}} U ON U."NAME" = COALESCE(NULLIF(TRIM(QH."USER_NAME"),''), 'N/A')
    INNER JOIN {{ref("dim_warehouse")}} W ON W."WAREHOUSE_NAME" = COALESCE(NULLIF(TRIM(QH."WAREHOUSE_NAME"),''), 'N/A')
    INNER JOIN {{ref("access_history_vw")}} A ON A."QUERY_ID" = COALESCE(NULLIF(TRIM(QH."QUERY_ID"),''), 'N/A')
    INNER JOIN {{ref("dim_table")}} T ON T."TABLE_CATALOG" = COALESCE(NULLIF(TRIM(A."database_name"),''), 'N/A') AND
                                         T."TABLE_SCHEMA" = COALESCE(NULLIF(TRIM(A."schema_name"),''), 'N/A') AND
                                         T."TABLE_NAME" = COALESCE(NULLIF(TRIM(A."table_name"),''), 'N/A')
GROUP BY 
        AH."ACCESS_HISTORY_ID",
        D."DATABASE_ID",
        E."ERROR_ID",
        LH."LOGIN_HISTORY_ID",
        R."ROLES_ID",
        S."SCHEMA_ID",
        SE."COMBINED_SESSION_ID",
        T."TABLE_ID",
        TG."TAGS_ID",
        U."COMBINED_USERS_ID",
        W."COMBINED_WAREHOUSE_ID",
        QH."QUERY_ID",
        to_char(QH."START_TIME", 'YYYYMMDD'),
        to_char(QH."END_TIME", 'YYYYMMDD')
