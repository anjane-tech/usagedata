{#--------------------------------------------------------------
 Macro Overriding the global_project is_incremental() function.
 Paremeters: None 
----------------------------------------------------------------#}
{% macro is_incremental() %}
    {% if not execute %}
        {{ return(False) }}
    {% else %}
        {% set relation = adapter.get_relation(this.database, this.schema, this.table) %}
        {{ return(relation is not none
                  and relation.type == 'table'
                  and not should_full_refresh()) }}
    {% endif %}
{% endmacro %}