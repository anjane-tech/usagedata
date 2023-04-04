{{
  config(
    materialized='incremental',
    unique_key = '"STORAGE_TYPE_ID"',
    merge_update_columns = [var("col_update_dts"),'STORAGE_TYPE'],
    tags = ["dimensions"],
    schema = var("usage_data_datamart_schema_name")
  )
}}

with dimension as(
  select distinct 
    "STORAGE_TYPE" 
  from {{ref('daily_spend')}}
)

SELECT
      {{dbt_utils.generate_surrogate_key(
          ['"STORAGE_TYPE"']
      )}} AS "STORAGE_TYPE_ID", *,
      current_timestamp as "{{var('col_create_dts')}}",
      current_timestamp as "{{var('col_update_dts')}}"
FROM dimension


