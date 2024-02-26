{{config(materialized='view',
        tags = ["staging"],
        schema = var("usage_data_staging_schema_name"))}}
        
 select * 
 from {{ref("errors")}}