{% macro divide_by_thousand(metrics) %}
    {{ metrics/1000 }}
{% endmacro %}