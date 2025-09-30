{% macro validate_regex(column_name_quoted, value_str, pattern) %}
    {% set column_name_unquoted = column_name_quoted | replace('"', '') | replace('`', '') %}
    case
        when {{ value_str }} is not null
             and not regexp_contains(trim({{ value_str }}), r'{{ pattern }}')
        then 'Column ' || '{{ column_name_unquoted }}' || ' failed regex match: ' || {{ value_str }}
        else null
    end
{% endmacro %}
