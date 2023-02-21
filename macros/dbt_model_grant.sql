{% macro dbt_model_grant() %}

{% if execute %}

{% set grant_query %}
    GRANT SELECT ON {{this}} to dbtadmin;
    commit;
{% endset %}

{% do run_query(grant_query) %}

{% endif %}

{% endmacro %}