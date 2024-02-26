{{config(
           materialized='incremental',
           tags = ["staging"],
           schema = var("usage_data_staging_schema_name"),
           pre_hook = insert_usagedata_metadata("staging", '"END_TIME"', this)
        )
}}

 select qh.*
 from {{source(var("source"),var("sf_tbl_query_history"))}} qh 
   inner join {{source(var("source"),var("sf_tbl_access_history"))}} ah on ah.QUERY_ID = qh.QUERY_ID
 WHERE
 "END_TIME" <> null 
 