{{
  config(
    materialized='incremental',
    unique_key = '"COMBINED_WAREHOUSE_ID"',
    merge_update_columns = [var("col_update_dts"),'AUTO_SUSPEND','STATE','WAREHOUSE_SIZE','MIN_CLUSTER_COUNT','UUID'],
    tags = ["dimensions"],
    schema = var("usage_data_entityattribute_schema_name")
  )
}}

with warehouse as (
    SELECT * 
    FROM {{ref('warehouse_stage_vw')}}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY "WAREHOUSE_NAME" ORDER BY UPDATED_ON DESC) = 1
),

query_history as (
    select * 
    from {{ref('query_history_stage')}}
),

dimension as(
    SELECT DISTINCT
           CASE WHEN NULLIF(TRIM(w."WAREHOUSE_NAME"),'') IS NOT NULL THEN W."WAREHOUSE_NAME"
                WHEN NULLIF(TRIM(qh."WAREHOUSE_NAME"),'') IS NOT NULL THEN QH."WAREHOUSE_NAME"
                ELSE 'N/A' END AS "WAREHOUSE_NAME",
           COALESCE(w."MIN_CLUSTER_COUNT", 0) AS "MIN_CLUSTER_COUNT",
           COALESCE(w."MAX_CLUSTER_COUNT", 0) AS "MAX_CLUSTER_COUNT",
           w."AUTO_SUSPEND" AS "AUTO_SUSPEND",
           w."AUTO_RESUME" AS "AUTO_RESUME",
           w."UUID" AS "UUID",
           COALESCE(w."OWNER", 'N/A') AS "OWNER",
           w."STATE" AS "STATE",
           CASE WHEN w."SIZE" IS NOT NULL THEN W."SIZE"
                WHEN qh."WAREHOUSE_SIZE" IS NOT NULL THEN QH."WAREHOUSE_SIZE"
                ELSE NULL END AS "WAREHOUSE_SIZE",
           CASE WHEN w."TYPE" IS NOT NULL THEN W."TYPE"
                WHEN qh."WAREHOUSE_TYPE" IS NOT NULL THEN QH."WAREHOUSE_TYPE"
                ELSE NULL END AS "WAREHOUSE_TYPE"
    FROM query_history qh
    FULL OUTER JOIN warehouse w on W."WAREHOUSE_NAME" = QH."WAREHOUSE_NAME"
)

SELECT
     'warehouse' as entity_name,
     {{ dbt_utils.generate_surrogate_key(
         ['"WAREHOUSE_NAME"']
     )}} as id,
     WAREHOUSE_NAME as attribute_1,
     MIN_CLUSTER_COUNT as attribute_2,
     MAX_CLUSTER_COUNT as attribute_3,
     AUTO_SUSPEND as attribute_4,
     AUTO_RESUME as attribute_5,
     UUID as attribute_6,
     OWNER as attribute_7,
    STATE as attribute_8,
    WAREHOUSE_SIZE as attribute_9,
    WAREHOUSE_TYPE as attribute_10,
     current_timestamp as "{{var('col_create_dts')}}",
      current_timestamp as "{{var('col_update_dts')}}"

FROM dimension