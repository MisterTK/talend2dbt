{% macro parse_dynamic_timestamp(timestamp_expr) %}
(
  case
    when safe.parse_timestamp('%y-%m-%d %h:%m:%s', {{ timestamp_expr }}) is not null
      then safe.parse_timestamp('%y-%m-%d %h:%m:%s', {{ timestamp_expr }})
    when safe.parse_timestamp('%y-%m-%dt%h:%m:%s', {{ timestamp_expr }}) is not null
      then safe.parse_timestamp('%y-%m-%dt%h:%m:%s', {{ timestamp_expr }})
    when safe.parse_timestamp('%y%m%d %h:%m:%s', {{ timestamp_expr }}) is not null
      then safe.parse_timestamp('%y%m%d %h:%m:%s', {{ timestamp_expr }})
    when safe.parse_timestamp('%y%m%dt%h:%m:%s', {{ timestamp_expr }}) is not null
      then safe.parse_timestamp('%y%m%dt%h:%m:%s', {{ timestamp_expr }})
    else null
  end
)
{% endmacro %}
