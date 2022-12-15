{{config(materialized='view',
        tags = ["staging"])}}
        
 select * 
 from {{ref("errors_seed")}}