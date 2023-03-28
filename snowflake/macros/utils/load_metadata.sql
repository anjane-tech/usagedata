{% macro insert_usagedata_metadata(source, column, table_identifier) %}

    {% set sql %}
       INSERT INTO {{ref('usagedata_metadata')}}
       SELECT S.*
       FROM (
        SELECT '{{source}}' as Source, MAX({{column}}) as "{{var('col_update_dts')}}"
        FROM {{table_identifier}}
        GROUP BY 1) S, {{ref('usagedata_metadata')}} T
       WHERE S.Source = T.Source and S."{{var('col_update_dts')}}" > T."{{var('col_update_dts')}}";
    {% endset %}
    {% if execute and flags.WHICH in ('run', 'build') and is_incremental() %}
       {% do run_query(sql) %}
       {% do log("Load Metadata updated for Query History", info=True) %}
    {% endif %}
{% endmacro %}
