{{config(materialized='table',
        tags = ["staging"])}}

select ah.* 
from {{source(var("source"),var("sf_tbl_access_history"))}} ah
   inner join {{ref("query_history_stage")}} qh on ah.QUERY_ID = qh.QUERY_ID
 