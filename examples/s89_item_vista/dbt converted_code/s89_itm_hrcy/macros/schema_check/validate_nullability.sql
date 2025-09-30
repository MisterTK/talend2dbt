{% macro validate_nullability(column_name_quoted, is_nullable, value_str) %}
    {{ log("is_nullable:" ~ is_nullable, info=True) }}
    {% if not is_nullable %}
        {{ log("column_name_quoted:" ~ column_name_quoted, info=True) }}
        {{ log("value_str:" ~ value_str ~ "..", info=True) }}
        {%- set column_name_unquoted = column_name_quoted | replace('"', '') | replace('`', '') -%}
        case
            when {{ value_str }} is null
              or trim({{ value_str }}) = ''
              or lower(trim({{ value_str }})) in ('null', 'none')
            then 'Column ' || '{{ column_name_unquoted }}' || ' violates nullability constraint'
            else null
        end
    {% else %}
        null
    {% endif %}
{% endmacro %}