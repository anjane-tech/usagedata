
{{
  config(
    materialized='incremental',
    merge_update_columns = ['var("col_update_dts")','SPEND','SPEND_NET_CLOUD_SERVICES'],
    tags = ["fact"],
    schema = var("usage_data_datamart_schema_name")
  )
}}



SELECT
        ds."DATE",
        {{dbt_utils.generate_surrogate_key(["ds.SERVICE"])}} "SERVICE_ID",
        {{dbt_utils.generate_surrogate_key(["ds.STORAGE_TYPE"])}} "STORAGE_TYPE_ID",
        {{dbt_utils.generate_surrogate_key(["ds.WAREHOUSE_NAME"])}} "WAREHOUSE_ID",
        {{dbt_utils.generate_surrogate_key(["ds.DATABASE_NAME"])}} "DATABASE_ID",
        ds."currency",
        sum(ds."SPEND"::DECIMAL(38, 5)) as "spend",
        sum(ds."SPEND_NET_CLOUD_SERVICES"::DECIMAL(38, 5)) as "spend_net_cloud_services",
        current_timestamp as "{{var('col_create_dts')}}",
        current_timestamp as "{{var('col_update_dts')}}"
 FROM  {{ref('daily_spend')}} ds
 
 GROUP BY
      ds."DATE",
      ds."SERVICE",
      ds."STORAGE_TYPE",
      ds."WAREHOUSE_NAME",
      ds."DATABASE_NAME",
      ds."currency"