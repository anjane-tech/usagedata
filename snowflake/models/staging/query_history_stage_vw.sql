{{config(materialized='table',
        tags = ["staging"])}}

 select *
 from {{source(var("source"),var("sf_tbl_query_history"))}}
 WHERE "END_TIME" > (SELECT max("{{var('col_update_dts')}}") FROM {{ref('usagedata_metadata')}})