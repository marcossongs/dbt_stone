{% macro set_last_execution() %}

{% if execute %}

begin;
DELETE FROM dbt_dw_log.dbt_incremental_control WHERE dbt_schema_nm = '{{ this.schema }}' and dbt_model_nm = '{{ this.table }}';
commit;

begin;
INSERT INTO dbt_dw_log.dbt_incremental_control(dbt_model_nm, dbt_schema_nm, last_execution_dt)

with
table_aux as (
    select max(source_updated_dt) last_execution_dt from {{this}}
)
SELECT
    '{{ this.table }}' as dbt_model_nm,
    '{{ this.schema }}' as dbt_schema_nm,
    isnull(table_aux.last_execution_dt, to_timestamp('1990-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')) as last_execution_dt
FROM table_aux
;
commit;

{% endif %}

{% endmacro %}