{% macro validate_date_type(column_name_quoted, value_str) %}
    {%- set column_name_unquoted = column_name_quoted | replace('"', '') | replace('`', '') -%}
    case
        when {{ value_str }} is null 
          or trim({{ value_str }}) = '' 
          or lower({{ value_str }}) in ('null', 'none', '0000-00-00')
        then null
        when {{ parse_dynamic_date(value_str) }} is null
        then 'Column {{ column_name_unquoted }} contains invalid date format: ' || coalesce({{ value_str }}, 'null')
        else null
    end
{% endmacro %}