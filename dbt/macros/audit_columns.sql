{% macro audit_columns() %}
{#-
    Standard enterprise metadata audit attributes.
-#}
CURRENT_TIMESTAMP AS dbt_updated_at,
'{{ invocation_id }}' AS dbt_invocation_id
{% endmacro %}

