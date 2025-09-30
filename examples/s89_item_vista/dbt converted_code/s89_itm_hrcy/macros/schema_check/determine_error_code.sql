{% macro determine_error_code(aggregated_error_sql) %}
    case 
        when {{ aggregated_error_sql }} is not null and length({{ aggregated_error_sql }}) > 0
        then false
        else true
    end
{% endmacro %}
