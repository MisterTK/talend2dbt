{% macro cast_columns(intermdiate_target_table_project, intermdiate_target_table_dataset, intermdiate_target_table_name) %}
  {%- set query %}
    SELECT 
      column_name, 
      data_type
    FROM `{{ intermdiate_target_table_project }}.{{ intermdiate_target_table_dataset }}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = '{{ intermdiate_target_table_name }}'
    ORDER BY ordinal_position
  {%- endset %}

  {%- set columns_info = dbt_utils.get_query_results_as_dict(query) -%}

  {%- if columns_info and (columns_info.values() | first | length) > 0 -%}
    {%- set cast_exprs = [] -%}
    {%- for i in range(columns_info['column_name'] | length) -%}
      {%- set col_name = columns_info['column_name'][i] -%}
      {%- set data_type = columns_info['data_type'][i] | lower -%}

      {%- if col_name and data_type -%}

        {# ----------- DATE TYPE ----------- #}
        {% if data_type == 'date' %}
          {% set expr %}
            case
              when REGEXP_CONTAINS({{ col_name }}, r'^\d{4}-\d{2}-\d{2}$') then SAFE.PARSE_DATE('%Y-%m-%d', {{ col_name }})
              when REGEXP_CONTAINS({{ col_name }}, r'^\d{2}/\d{2}/\d{4}$') then SAFE.PARSE_DATE('%m/%d/%Y', {{ col_name }})
              when REGEXP_CONTAINS({{ col_name }}, r'^\d{4}/\d{2}/\d{2}$') then SAFE.PARSE_DATE('%Y/%m/%d', {{ col_name }})
              when REGEXP_CONTAINS({{ col_name }}, r'^\d{2}-\d{2}-\d{4}$') then SAFE.PARSE_DATE('%d-%m-%Y', {{ col_name }})
              when REGEXP_CONTAINS({{ col_name }}, r'^\d{8}$') then SAFE.PARSE_DATE('%Y%m%d', {{ col_name }})
              else null
            end AS {{ col_name }}
          {% endset %}
        
        {# ----------- DATETIME TYPE ----------- #}
        {% elif data_type == 'datetime' %}
          {% set expr %}
            case
              when REGEXP_CONTAINS({{ col_name }}, r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$') then SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', {{ col_name }})
              when REGEXP_CONTAINS({{ col_name }}, r'^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$') then SAFE.PARSE_DATETIME('%m/%d/%Y %H:%M:%S', {{ col_name }})
              when REGEXP_CONTAINS({{ col_name }}, r'^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}$') then SAFE.PARSE_DATETIME('%Y/%m/%d %H:%M:%S', {{ col_name }})
              else null
            end AS {{ col_name }}
          {% endset %}

        {# ----------- TIMESTAMP TYPE ----------- #}
        {% elif data_type == 'timestamp' %}
          {% set expr %}
            case
              when REGEXP_CONTAINS(cast({{ col_name }} as STRING), r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z)?$') then SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', cast({{ col_name }} as STRING))
              when REGEXP_CONTAINS(cast({{ col_name }} as STRING), r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$') then SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', cast({{ col_name }} as STRING))
              when REGEXP_CONTAINS(cast({{ col_name }} as STRING), r'^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$') then SAFE.PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S', cast({{ col_name }} as STRING))
              else SAFE_CAST(CAST({{ col_name }} AS STRING) AS TIMESTAMP)
            end AS {{ col_name }}
          {% endset %}

        {# ----------- OTHER TYPES ----------- #}
        {% else %}
          {% set expr = "SAFE_CAST(" ~ col_name ~ " AS " ~ data_type ~ ") AS " ~ col_name %}
        {% endif %}

        {%- do cast_exprs.append(expr) -%}
      {%- endif -%}
    {%- endfor -%}
    {{ cast_exprs | join(",\n  ") }}
  {%- else -%}
    {{ return("*") }}
  {%- endif -%}
{% endmacro %}
