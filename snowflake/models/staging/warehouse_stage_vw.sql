{{config(materialized='view',
        tags = ["staging"],
        pre_hook = [generate_warehouse_metadata()]
        )
}}
 
 select *
 from {{source(var("source"),var("sf_tbl_warehouse"))}}