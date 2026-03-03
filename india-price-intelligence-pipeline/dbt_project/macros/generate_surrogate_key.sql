{% macro generate_surrogate_key(columns) %}
    to_hex(md5(concat({{ columns | join(", ") }})))
{% endmacro %}
