{% macro dynamic_table_name(prefix) %}
  {{ prefix }}_{{ modules.datetime.datetime.now().strftime("%Y%m%d%H%M%S") }}_psv
{% endmacro %}
