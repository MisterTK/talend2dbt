{% macro parse_dynamic_date(date_expr) %}
(
  case
    when safe.parse_date('%Y-%m-%d', {{ date_expr }}) is not null then safe.parse_date('%Y-%m-%d', {{ date_expr }})
    when safe.parse_date('%d/%m/%Y', {{ date_expr }}) is not null then safe.parse_date('%d/%m/%Y', {{ date_expr }})
    when safe.parse_date('%m-%d-%Y', {{ date_expr }}) is not null then safe.parse_date('%m-%d-%Y', {{ date_expr }})
    when safe.parse_date('%b %d, %Y', {{ date_expr }}) is not null then safe.parse_date('%b %d, %Y', {{ date_expr }})
    when safe.parse_date('%Y%m%d', {{ date_expr }}) is not null then safe.parse_date('%Y%m%d', {{ date_expr }})
    else null
  end
)
{% endmacro %}