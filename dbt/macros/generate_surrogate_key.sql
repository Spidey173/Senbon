{% macro generate_surrogate_key(field_list) %}
{#-
    Generates a deterministic MD5 hash surrogate key across one or more fields.
    Standard Kimball dimensional modeling practice.
-#}
MD5(
    CONCAT(
        {%- for field in field_list %}
        COALESCE(CAST({{ field }} AS VARCHAR), '_NULL_')
        {%- if not loop.last %}, '|', {% endif -%}
        {%- endfor %}
    )
)
{% endmacro %}

