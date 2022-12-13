{{
    config(materialized='incremental',
           tags = ["staging", "metadata"]
          )
}}
 
 select 'staging' as Source,
       {% if is_incremental() %}
           to_timestamp_ntz(current_timestamp()) as "{{var('col_update_dts')}}"
       {% else %}
           to_timestamp_ntz('1901-01-01') as "{{var('col_update_dts')}}"
       {% endif %}
 {% if is_incremental() %}
 where 1 = 2
 {% endif %}
 