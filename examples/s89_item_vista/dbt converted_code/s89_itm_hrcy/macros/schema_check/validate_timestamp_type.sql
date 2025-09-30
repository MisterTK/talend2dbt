{% macro validate_timestamp_type(column_name_quoted, value_str) %}
  {# Remove quotes from column name for clean error messages #}
  {%- set column_name_unquoted = column_name_quoted | replace('"', '') | replace('`', '') -%}

  case
    when {{ value_str }} is null 
         or trim({{ value_str }}) = '' 
         or lower({{ value_str }}) in ('null', 'none', '0000-00-00 00:00:00')
    then null

    when safe.parse_timestamp('%Y-%m-%d %H:%M:%S', {{ value_str }}) is null
         and safe.parse_timestamp('%Y-%m-%dT%H:%M:%S', {{ value_str }}) is null
         and safe.parse_timestamp('%Y%m%d %H:%M:%S', {{ value_str }}) is null
         and safe.parse_timestamp('%Y%m%dT%H:%M:%S', {{ value_str }}) is null
    then
      'Column ' || '{{ column_name_unquoted }}' || 
      ' (' || coalesce({{ value_str }}, 'null') || ') contains invalid timestamp format.'
    else null
  end
{% endmacro %}