
{{
  config(
    materialized='incremental',
    merge_update_columns = ['var("col_update_dts")','WAREHOUSE_ID','DATABASE_ID','SERVICE_ID','STORAGE_TYPE_ID'],
    tags = ["fact"],
    schema = var("usage_data_datamart_schema_name")
  )
}}



SELECT
        daily_spend.date,
        {{dbt_utils.generate_surrogate_key(["daily_spend.SERVICE"])}} "SERVICE_ID",
        {{dbt_utils.generate_surrogate_key(["daily_spend.STORAGE_TYPE"])}} "STORAGE_TYPE_ID",
        {{dbt_utils.generate_surrogate_key(["daily_spend.WAREHOUSE_NAME"])}} "WAREHOUSE_ID",
        {{dbt_utils.generate_surrogate_key(["daily_spend.DATABASE_NAME"])}} "DATABASE_ID",
        daily_spend."currency",
        sum(daily_spend."SPEND"::DECIMAL(38, 5)) as "spend",
        sum(daily_spend."SPEND_NET_CLOUD_SERVICES"::DECIMAL(38, 5)) as "spend_net_cloud_services",
        current_timestamp as "{{var('col_create_dts')}}",
        current_timestamp as "{{var('col_update_dts')}}"
 from  {{ref('daily_spend')}} daily_spend
 group by 1,2,3,4,5,6
    
    
        