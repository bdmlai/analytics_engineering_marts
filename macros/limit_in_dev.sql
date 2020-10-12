{% macro limit_in_dev(timestamp) %}
    -- this filter will only apply during a dev run
    {% if target.name == 'dev' %}
    
    where {{ timestamp }} >= dateadd('month', -1, current_date)
    
    {% endif %}
    
{% endmacro %}