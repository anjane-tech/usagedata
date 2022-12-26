{% macro generate_warehouse_metadata() %}
  {{return(adapter.dispatch('generate_warehouse_metadata')())}}
{% endmacro %}

{% macro default__generate_warehouse_metadata() %}
{% endmacro %}

{% macro snowflake__generate_warehouse_metadata() %}

  {% set sql %}
      show warehouses;
      create or replace transient table {{var('usage_data_staging_schema_name')}}.WAREHOUSE_METADATA COPY GRANTS as (select "name" AS "WAREHOUSE_NAME", "state" AS  "STATE", "type" AS  "TYPE", "size" AS  "SIZE", "min_cluster_count" AS  "MIN_CLUSTER_COUNT", "max_cluster_count" AS  "MAX_CLUSTER_COUNT", "started_clusters" AS  "STARTED_CLUSTERS", "running" AS  "RUNNING", "queued" AS  "QUEUED", "is_default" AS  "IS_DEFAULT", "is_current" AS  "IS_CURRENT", "auto_suspend" AS  "AUTO_SUSPEND", "auto_resume" AS  "AUTO_RESUME", "available" AS  "AVAILABLE", "provisioning" AS  "PROVISIONING", "quiescing" AS  "QUIESCING", "other" AS  "OTHER", "created_on" AS  "CREATED_ON", "resumed_on" AS  "RESUMED_ON", "updated_on" AS  "UPDATED_ON", "owner" AS  "OWNER", "comment" AS  "COMMENT", "enable_query_acceleration" AS  "ENABLE_QUERY_ACCELERATION", "query_acceleration_max_scale_factor" AS  "QUERY_ACCELERATION_MAX_SCALE_FACTOR", "resource_monitor" AS  "RESOURCE_MONITOR", "actives" AS  "ACTIVES", "pendings" AS  "PENDINGS", "failed" AS  "FAILED", "suspended" AS  "SUSPENDED", "uuid" AS  "UUID", "scaling_policy" AS  "SCALING_POLICY" from table(result_scan(last_query_id())));
  {% endset %}
  {% if execute %}
    {% do run_query(sql) %}
    {% do log("Warehouse Stage created", info=True) %}
  {% endif %}

{% endmacro %}
