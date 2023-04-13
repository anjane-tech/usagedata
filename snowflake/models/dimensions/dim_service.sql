



{{
  config(
    materialized='incremental',
    unique_key = '"SERVICE_ID"',
    merge_update_columns = [var("col_update_dts"),'SERVICE'],
    tags = ["dimensions"],
    schema = var("usage_data_datamart_schema_name")
  )
}}
with
 
dimensions AS(SELECT DISTINCT SERVICE from {{ref('daily_spend')}})

SELECT 
     {{ dbt_utils.generate_surrogate_key(
         ['"SERVICE"']
     )}} as "SERVICE_ID", *,
     current_timestamp as "{{var('col_create_dts')}}",
        current_timestamp as "{{var('col_update_dts')}}"
FROM dimensions