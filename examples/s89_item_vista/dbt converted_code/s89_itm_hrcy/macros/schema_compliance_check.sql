{% macro schema_compliance_check(source_table, table_name) %}

{%- set rules_dict = dbt_utils.get_query_results_as_dict(
    "select * from " ~ ref('stg_schema_rules') ~ " where table_name = '" ~ table_name ~ "'"
) -%}

{%- set rules = [] -%}
{%- for i in range(rules_dict.values() | first | length) -%}
  {%- set row = {} -%}
  {%- for col in rules_dict -%}
    {%- set _ = row.update({ col: rules_dict[col][i] }) -%}
  {%- endfor -%}
  {%- do rules.append(row) -%}
{%- endfor -%}

{%- if rules | length > 0 -%}
  {% for rule in rules %}
    {% do log("🧪 Rule: " ~ rule, info=True) %}
  {% endfor %}
{%- endif -%}

with raw as (
  select * from {{ source_table }}
),

validated as (
  select
    raw.*,

    -- Error code: short error identifier
    case
      {%- for rule in rules %}
        {%- set column = rule['column_name'] %}
        {%- set expected_type = rule['expected_type'] %}
        {%- set is_nullable = rule['nullable'] in ['true', 'True', 'TRUE', True, 1, '1'] %}
        {%- set max_length = rule['max_length'] %}
        {%- set precision = rule['precision'] %}
        {%- set format = rule['format'] %}

        {%- if column and expected_type %}
          {%- if not is_nullable %}
            when nullif({{ column }}, '') IS NULL then 'NULL_NOT_ALLOWED'
          {%- endif %}

          when (
            nullif({{ column }}, '') IS NOT NULL
            AND SAFE_CAST(nullif({{ column }}, '') AS {{ expected_type }}) IS NULL
          ) then 'INVALID_TYPE'

          {%- if max_length %}
            when CHAR_LENGTH({{ column }}) > {{ max_length }} then 'TOO_LONG'
          {%- endif %}

          {%- if precision is not none %}
            when LENGTH(SPLIT(nullif({{ column }}, ''), '.')[SAFE_OFFSET(1)]) > {{ precision | int }}
            then 'PRECISION_EXCEEDED'
          {%- endif %}

          {%- if expected_type == 'DATE' and format %}
            when SAFE.PARSE_DATE('{{ format }}', nullif({{ column }}, '')) IS NULL then 'BAD_DATE_FORMAT'
          {%- endif %}
        {%- endif %}
      {%- endfor %}
      else null
    end as error_code,

    -- Error message: human-friendly description
    case
      {%- for rule in rules %}
        {%- set column = rule['column_name'] %}
        {%- set expected_type = rule['expected_type'] %}
        {%- set is_nullable = rule['nullable'] in ['true', 'True', 'TRUE', True, 1, '1'] %}
        {%- set max_length = rule['max_length'] %}
        {%- set precision = rule['precision'] %}
        {%- set format = rule['format'] %}

        {%- if column and expected_type %}
          {%- if not is_nullable %}
            when nullif({{ column }}, '') IS NULL then 'NULL not allowed: {{ column }}'
          {%- endif %}

          when (
            nullif({{ column }}, '') IS NOT NULL
            AND SAFE_CAST(nullif({{ column }}, '') AS {{ expected_type }}) IS NULL
          ) then 'Invalid type: {{ column }}'

          {%- if max_length %}
            when CHAR_LENGTH({{ column }}) > {{ max_length }} then 'Too long: {{ column }}'
          {%- endif %}

          {%- if precision is not none %}
            when LENGTH(SPLIT(nullif({{ column }}, ''), '.')[SAFE_OFFSET(1)]) > {{ precision | int }}
            then 'Precision exceeded: {{ column }}'
          {%- endif %}

          {%- if expected_type == 'DATE' and format %}
            when SAFE.PARSE_DATE('{{ format }}', nullif({{ column }}, '')) IS NULL then 'Bad date format: {{ column }}'
          {%- endif %}
        {%- endif %}
      {%- endfor %}
      else null
    end as error_message

  from raw
)

select * from validated

{% endmacro %}
