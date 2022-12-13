{{config(materialized='table',
        tags = ["staging"])}}

select ah.QUERY_ID,
       ah.QUERY_START_TIME,
       ah.USER_NAME,
       ah.DIRECT_OBJECTS_ACCESSED AS DIRECT_OBJECTS_ACCESSED,
       ah.BASE_OBJECTS_ACCESSED AS BASE_OBJECTS_ACCESSED,
       ah.OBJECTS_MODIFIED AS OBJECTS_MODIFIED
from {{source(var("source"),var("sf_tbl_access_history"))}} ah
   inner join {{ref("query_history_stage")}} qh on ah.QUERY_ID = qh.QUERY_ID
 