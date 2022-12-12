{{
  config(
    materialized='incremental',
    merge_update_columns = ['var("col_update_dts")','ACCESS_HISTORY_ID','DATABASE_ID','ERROR_ID','LOGIN_HISTORY_ID','ROLES_ID','SCHEMA_ID','COMBINED_SESSION_ID','TABLE_ID','TAGS_ID','COMBINED_USERS_ID','COMBINED_WAREHOUSE_ID','QUERY_ID'],
    tags = ["dimensions"]
  )
}}

  WITH query_history AS (
    SELECT * FROM {{ref("query_history_stage")}}
    {% if is_incremental() %}
    where "END_TIME" > (select max("{{var('col_update_dts')}}") from {{this}})
    {% endif %}
  )

SELECT 
        current_timestamp as "{{var('col_create_dts')}}",
        current_timestamp as "{{var('col_update_dts')}}",
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
        SUM(QH."BYTES_SCANNED"::DECIMAL(38, 0)) AS "BYTES_SCANNED",
        SUM(QH."OUTBOUND_DATA_TRANSFER_BYTES"::DECIMAL(38, 0)) AS "OUTBOUND_DATA_TRANSFER_BYTES",
        SUM(QH."INBOUND_DATA_TRANSFER_BYTES"::DECIMAL(38, 0)) AS "INBOUND_DATA_TRANSFER_BYTES",
        SUM(QH."EXTERNAL_FUNCTION_TOTAL_SENT_BYTES"::DECIMAL(38, 0)) AS "EXTERNAL_FUNCTION_TOTAL_SENT_BYTES"
 --       SUM(T."BYTES"::DECIMAL(38, 0)) AS "BYTES"
    FROM query_history QH 
    INNER JOIN {{ref("dim_access_history")}} AH ON AH."QUERY_ID" = COALESCE(QH."QUERY_ID", 'N/A')
    INNER JOIN {{ref("dim_database")}} D ON D."DATABASE_NAME" = COALESCE(QH."DATABASE_NAME", 'N/A')
    INNER JOIN {{ref("dim_error")}} E ON E."ERROR_CODE" = COALESCE(QH."ERROR_CODE", 'N/A')
    INNER JOIN {{ref("dim_sessions")}} SE ON SE."SESSION_ID" = COALESCE(QH."SESSION_ID", 'N/A')
    INNER JOIN {{ref("dim_login_history")}} LH ON LH."EVENT_ID" = COALESCE(SE."LOGIN_EVENT_ID", 'N/A')
    INNER JOIN {{ref("dim_roles")}} R ON R."NAME" = COALESCE(QH."ROLE_NAME", 'N/A')
    INNER JOIN {{ref("dim_schema")}} S ON S."CATALOG_NAME" = COALESCE(QH."DATABASE_NAME", 'N/A') AND
                                           S."SCHEMA_NAME" = COALESCE(QH."SCHEMA_NAME", 'N/A')
    INNER JOIN {{ref("dim_tags")}} TG ON TG."TAG_NAME" = COALESCE(QH."QUERY_TAG", 'N/A') AND
                                       TG."TAG_SCHEMA" = COALESCE(QH."SCHEMA_NAME", 'N/A') AND
                                     TG."TAG_DATABASE" = COALESCE(QH."DATABASE_NAME", 'N/A')
    INNER JOIN {{ref("dim_users")}} U ON U."NAME" = COALESCE(QH."USER_NAME", 'N/A')
    INNER JOIN {{ref("dim_warehouse")}} W ON W."WAREHOUSE_NAME" = COALESCE(QH."WAREHOUSE_NAME", 'N/A')
    INNER JOIN {{ref("access_history_temp")}} A ON A."QUERY_ID" = COALESCE(QH."QUERY_ID", 'N/A')

    INNER JOIN {{ref("dim_table")}} T ON T."TABLE_CATALOG" = COALESCE(A."database_name", 'N/A') AND
                                         T."TABLE_SCHEMA" = COALESCE(A."schema_name", 'N/A') AND
                                         T."TABLE_NAME" = COALESCE(A."table_name", 'N/A')
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
        QH."QUERY_ID"
