{% macro generate_warehouse_metadata() %}
   {{log("**** Going to generate the warehouse staging table ")}}
   {% set sql %}
     show warehouses;
     create or replace transient table USAGEDATA_{{var('usage_data_staging_schema_name')}}.warehouse_metadata as (select * from table(result_scan(last_query_id())));
   {% endset %}
   {% do run_query(sql) %}
   {% do log("Warehouse Stage created", info=True) %}
{% endmacro %}
