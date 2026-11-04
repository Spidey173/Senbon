{% macro cents_to_dollars(column_name, scale=2) %}
    ROUND(CAST(({{ column_name }} / 100.0) AS NUMERIC), {{ scale }})
{% endmacro %}

