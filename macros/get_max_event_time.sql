{% macro get_max_event_time() %}

{% if execute and is_incremental() %}

{% set source_query %}
    /*SELECT max(source_updated) FROM {{ this }};*/
    SELECT isnull(max(last_execution_dt), to_timestamp('1990-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')) source_updated_dt 
    FROM dbt_dw_log.dbt_incremental_control 
    WHERE dbt_schema_nm = '{{ this.schema }}'
      AND dbt_model_nm = '{{ this.table }}';
{% endset %}

{% set max_event_time = run_query(source_query).columns[0][0] %}

{% do return(max_event_time) %}

{% endif %}

{% endmacro %}