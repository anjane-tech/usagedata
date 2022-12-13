{# 
   --------- json_parse_udf ----------
#}
{% macro json_parse_udf(COLUMN_NAME, OBJECT_NAME, DATATYPE, OBJECT_NAME_ALIAS) -%}
   {{ return(adapter.dispatch('json_parse_udf')(COLUMN_NAME, OBJECT_NAME, DATATYPE, OBJECT_NAME_ALIAS)) }}
{%- endmacro %}

{% macro default__json_parse_udf(COLUMN_NAME, OBJECT_NAME, DATATYPE, OBJECT_NAME_ALIAS) -%}
    "{{COLUMN_NAME}}":{{OBJECT_NAME}}::{{DATATYPE}} as "{{OBJECT_NAME_ALIAS}}"
{%- endmacro %}

{% macro snowflake__json_parse_udf(COLUMN_NAME, OBJECT_NAME, DATATYPE, OBJECT_NAME_ALIAS) -%}
    "{{COLUMN_NAME}}":"{{OBJECT_NAME}}"::{{DATATYPE}} as "{{OBJECT_NAME_ALIAS}}"
{%- endmacro %}

{% macro postgres__json_parse_udf(COLUMN_NAME, OBJECT_NAME, DATATYPE, OBJECT_NAME_ALIAS) -%}
    ("{{COLUMN_NAME}}"::json->'{{OBJECT_NAME}}')::{{DATATYPE}} as "{{OBJECT_NAME_ALIAS}}"
{%- endmacro %}

{# 
   --------- json_parse_array_udf ----------
#}
{% macro json_parse_array_udf(OBJECT_NAME, OBJECT_NAME_ALIAS) -%}
   {{ return(adapter.dispatch('json_parse_array_udf')(OBJECT_NAME, OBJECT_NAME_ALIAS)) }}
{%- endmacro %}

{% macro default__json_parse_array_udf(OBJECT_NAME, OBJECT_NAME_ALIAS) -%}
    json_array_elements("{{OBJECT_NAME}}"::json) as "{{OBJECT_NAME_ALIAS}}"
{%- endmacro %}

{% macro snowflake__json_parse_array_udf(OBJECT_NAME, OBJECT_NAME_ALIAS) -%}
    PARSE_JSON(value) as "{{OBJECT_NAME_ALIAS}}"
{%- endmacro %}

{% macro postgres__json_parse_array_udf(OBJECT_NAME, OBJECT_NAME_ALIAS) -%}
    json_array_elements("{{OBJECT_NAME}}"::json) as "{{OBJECT_NAME_ALIAS}}"
{%- endmacro %}

{# 
   --------- flatten_json ----------
#}
{% macro flatten_json(OBJECT_NAME) -%}
   {{ return(adapter.dispatch('flatten_json')(OBJECT_NAME)) }}
{%- endmacro %}

{% macro default__flatten_json(OBJECT_NAME) -%}
{%- endmacro %}

{% macro snowflake__flatten_json(OBJECT_NAME) -%}
    , LATERAL flatten(input => parse_json("{{OBJECT_NAME}}"))
{%- endmacro %}