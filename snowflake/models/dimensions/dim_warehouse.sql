{{
  config(
    materialized='incremental',
    unique_key = '"COMBINED_WAREHOUSE_ID"',
    merge_update_columns = ['var("col_update_dts")','AUTO_SUSPEND','STATE','SIZE','MIN_CLUSTER_COUNT','UUID','WAREHOUSE_SIZE'],
    tags = ["dimensions"]
  )
}}

with warehouse as (
    SELECT * 
    FROM {{ref('warehouse_stage')}}
),

query_history as (
    select * 
    from {{ref('query_history_stage')}}
),

dimension as(
    SELECT DISTINCT
           CASE WHEN w."WAREHOUSE_NAME" IS NOT NULL THEN W."WAREHOUSE_NAME"
                WHEN qh."WAREHOUSE_NAME" IS NOT NULL THEN QH."WAREHOUSE_NAME"
                ELSE 'N/A' END AS "WAREHOUSE_NAME",
           COALESCE(w."MIN_CLUSTER_COUNT", 0) AS "MIN_CLUSTER_COUNT",
           COALESCE(w."MAX_CLUSTER_COUNT", 0) AS "MAX_CLUSTER_COUNT",
           COALESCE(w."AUTO_SUSPEND", 'N/A') AS "AUTO_SUSPEND",
           COALESCE(w."AUTO_RESUME", 'N/A') AS "AUTO_RESUME",
           COALESCE(w."UUID", 0) AS "UUID",
           COALESCE(w."OWNER", 'N/A') AS "OWNER",
           COALESCE(w."STATE", 'N/A') AS "STATE",
           CASE WHEN w."SIZE" IS NOT NULL THEN W."SIZE"
                WHEN qh."WAREHOUSE_SIZE" IS NOT NULL THEN QH."WAREHOUSE_SIZE"
                ELSE 'N/A' END AS "WAREHOUSE_SIZE",
           CASE WHEN w."TYPE" IS NOT NULL THEN W."TYPE"
                WHEN qh."WAREHOUSE_TYPE" IS NOT NULL THEN QH."WAREHOUSE_TYPE"
                ELSE 'N/A' END AS "WAREHOUSE_TYPE"
    FROM query_history qh
    FULL OUTER JOIN warehouse w on W."WAREHOUSE_NAME" = QH."WAREHOUSE_NAME"
)

SELECT
     {{ dbt_utils.generate_surrogate_key(
         ['"WAREHOUSE_NAME"']
     )}} as "COMBINED_WAREHOUSE_ID", *,
     current_timestamp as "{{var('col_create_dts')}}",
      current_timestamp as "{{var('col_update_dts')}}"

FROM dimension