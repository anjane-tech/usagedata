{% macro insert_usagedata_metadata(source, column, table_identifier) %}

      {% set sql %}
       INSERT INTO {{ref('usagedata_metadata')}}
       SELECT S.*
       FROM (
        SELECT '{{source}}' as Source, MAX({{column}}) as "{{var('col_update_dts')}}"
        FROM {{table_identifier}}
        GROUP BY 1) S left outer join {{ref('usagedata_metadata')}} T on S.Source = T.Source
        where S."{{var('col_update_dts')}}" > NVL(T."{{var('col_update_dts')}}", '1901-01-01 00:00:00');
    {% endset %}
    {% if execute and flags.WHICH in ('run', 'build') and is_incremental() %}
       {% do run_query(sql) %}
       {% do log("Load Metadata updated for Query History", info=True) %}
    {% endif %}
{% endmacro %}