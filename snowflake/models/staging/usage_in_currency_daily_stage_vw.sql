{{config(materialized='view',
        tags = ["staging"],
        schema = var("usage_data_staging_schema_name"))}}
 
 select *
 from {{source(var("source"),var("sf_tbl_usage_in_currency_daily"))}}
