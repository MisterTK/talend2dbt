{% macro validate_length(column_name_quoted, value_str, max_length) %}
    {% if max_length is defined and max_length is not none and (max_length | int) > 0 %}
        {%- set column_name_unquoted = column_name_quoted | replace('"', '') | replace('`', '') -%}
        case 
            when {{ value_str }} is not null and length({{ value_str }}) > cast('{{ max_length }}' as int64)
            then 'Column {{ column_name_unquoted }} exceeds max length of {{ max_length }}'
            else null
        end
    {% else %}
        null
    {% endif %}
{% endmacro %}
