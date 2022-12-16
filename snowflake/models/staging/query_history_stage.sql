{{config(materialized='table',
        tags = ["staging"])}}

 select qh.*
 from {{source(var("source"),var("sf_tbl_query_history"))}} qh 
   inner join {{source(var("source"),var("sf_tbl_access_history"))}} ah on ah.QUERY_ID = qh.QUERY_ID
 WHERE "END_TIME" > (SELECT max("{{var('col_update_dts')}}") FROM {{ref('usagedata_metadata')}})