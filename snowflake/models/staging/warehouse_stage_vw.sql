{{config(materialized='view',
        tags = ["staging"],
        pre_hook = [generate_warehouse_metadata()],
        schema = var("usage_data_staging_schema_name")
        )
}}
 
 select *
 from {{source(var("source"),var("sf_tbl_warehouse"))}}