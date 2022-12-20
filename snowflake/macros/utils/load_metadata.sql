{% macro insert_usagedata_metadata(source, column, table_identifier) %}
   {% set sql %}
       INSERT INTO {{ref('usagedata_metadata')}}
       SELECT '{{source}}' as Source, MAX({{column}}) as "{{var('col_update_dts')}}"
       FROM {{table_identifier}}
       GROUP BY 1;
   {% endset %}
   {% if execute %}
       {% do run_query(sql) %}
       {% do log("Load Metadata updated for Query History", info=True) %}
    {% endif %}
{% endmacro %}
