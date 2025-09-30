{% macro validate_integer_type(column_name_quoted, value_str) %}
    {%- set column_name_unquoted = column_name_quoted | replace('"', '') | replace('`', '') -%}
    case
        when {{ value_str }} is not null and safe_cast({{ value_str }} as int64) is null
        then 'Column {{ column_name_unquoted }} contains invalid integer value: ' || coalesce({{ value_str }}, 'null')
        else null
    end
{% endmacro %}
