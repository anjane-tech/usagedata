{{config(materialized='view',
        tags = ["staging"])}}
 
 select *
 from {{source(var("source"),var("sf_tbl_sessions"))}}