{{
  config(
    materialized='incremental',
    unique_key = '"DATABASE_ID"',
    merge_update_columns = ['var("col_update_dts")','DATABASE_NAME','DATABASE_OWNER','IS_TRANSIENT','COMMENT','LAST_ALTERED'],
    tags = ["dimensions"]
  )
}}

with db as (
    SELECT * 
    FROM {{ref('database_stage')}}
),

query_history as (
    SELECT * 
    FROM {{ref('query_history_stage')}}
    {% if is_incremental() %}
    WHERE "END_TIME" > (SELECT max("{{var('col_create_dts')}}") FROM {{this}})
    {% endif %}
),

dimension as (
    SELECT DISTINCT
            current_timestamp as "CREATEDTS",
            current_timestamp as "UPDATEDDTS",
           CASE WHEN d."DATABASE_NAME" IS NOT NULL THEN D."DATABASE_NAME"
                WHEN qh."DATABASE_NAME" IS NOT NULL THEN QH."DATABASE_NAME"
                ELSE 'N/A' END AS "DATABASE_NAME",
           COALESCE (d."DATABASE_OWNER", 'N/A') AS "DATABASE_OWNER",
           COALESCE (d."IS_TRANSIENT", 'N/A') AS "IS_TRANSIENT",
           COALESCE (d."COMMENT", 'N/A') AS "COMMENT",
           COALESCE (d."CREATED", 'N/A') AS "CREATED",
           COALESCE (d."LAST_ALTERED", 'N/A') AS "LAST_ALTERED"
    FROM query_history qh
    FULL OUTER JOIN db d ON D."DATABASE_NAME" = QH."DATABASE_NAME"
)

SELECT      
    {{dbt_utils.surrogate_key(
         ['"DATABASE_NAME"']
     )}} AS "DATABASE_ID", *
FROM dimension
