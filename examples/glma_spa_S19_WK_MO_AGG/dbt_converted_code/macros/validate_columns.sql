{% macro validate_columns(source_table, table_name) %}
{%- set rules_dict = dbt_utils.get_query_results_as_dict(
    "select * from " ~ ref('schema_rules') ~ " where table_name = '" ~ table_name ~ "'"
) -%}
{%- set rules = [] -%}
{%- for i in range(rules_dict.values() | first | length) -%}
  {%- set row = {} -%}
  {%- for col in rules_dict -%}
    {%- set _ = row.update({ col: rules_dict[col][i] }) -%}
  {%- endfor -%}
  {%- do rules.append(row) -%}
{%- endfor %}

with raw as (
  select * from {{ source_table }}
),

validated as (
  select
    raw.*,

    {% set error_conditions = [] %}
    {% set error_messages = [] %}
    {%- for rule in rules %}
      {%- set column = rule['column_name'] %}
      {%- set expected_type = rule['expected_type'] %}
      {%- set is_nullable = rule['nullable'] in ['true','True','TRUE',True,1,'1'] %}
      {%- set max_length = rule['max_length'] %}
      {%- set precision = rule['precision'] %}
      {%- set format = rule['format'] %}

      {%- if column and expected_type %}
        {%- if not is_nullable %}
          {% do error_conditions.append("WHEN nullif(CAST("~column~" AS STRING), '') IS NULL THEN 'NULL_NOT_ALLOWED'") %}
          {% do error_messages.append("WHEN nullif(CAST("~column~" AS STRING), '') IS NULL THEN 'NULL not allowed: "~column~"'") %}
        {%- endif %}

        {% if expected_type == 'DATE' and format %}
          {% do error_conditions.append(
            "WHEN CAST("~column~" AS STRING) IS NOT NULL AND TRIM(CAST("~column~" AS STRING)) != '' AND NOT REGEXP_CONTAINS(CAST("~column~" AS STRING), r'^[0-9]{8}$') THEN 'BAD_DATE_FORMAT'"
          ) %}
          {% do error_messages.append(
            "WHEN CAST("~column~" AS STRING) IS NOT NULL AND TRIM(CAST("~column~" AS STRING)) != '' AND NOT REGEXP_CONTAINS(CAST("~column~" AS STRING), r'^[0-9]{8}$') THEN 'Bad date format: "~column~"'"
          ) %}
        {%- else %}
          {% do error_conditions.append(
            "WHEN nullif(CAST("~column~" AS STRING), '') IS NOT NULL AND SAFE_CAST(nullif(CAST("~column~" AS STRING), '') AS "~expected_type~") IS NULL THEN 'INVALID_TYPE'"
          ) %}
          {% do error_messages.append(
            "WHEN nullif(CAST("~column~" AS STRING), '') IS NOT NULL AND SAFE_CAST(nullif(CAST("~column~" AS STRING), '') AS "~expected_type~") IS NULL THEN 'Invalid type: "~column~"'"
          ) %}
        {%- endif %}

        {%- if max_length %}
          {% do error_conditions.append(
            "WHEN CHAR_LENGTH(CAST("~column~" AS STRING)) > " ~ max_length ~ " THEN 'TOO_LONG'"
          ) %}
          {% do error_messages.append(
            "WHEN CHAR_LENGTH(CAST("~column~" AS STRING)) > " ~ max_length ~ " THEN 'Too long: " ~ column ~ "'"
          ) %}
        {%- endif %}

        {%- if precision is not none %}
          {% do error_conditions.append(
            "WHEN LENGTH(SPLIT(nullif(CAST("~column~" AS STRING), ''), '.')[SAFE_OFFSET(1)]) > " ~ precision ~ " THEN 'PRECISION_EXCEEDED'"
          ) %}
          {% do error_messages.append(
            "WHEN LENGTH(SPLIT(nullif(CAST("~column~" AS STRING), ''), '.')[SAFE_OFFSET(1)]) > " ~ precision ~ " THEN 'Precision exceeded: " ~ column ~ "'"
          ) %}
        {%- endif %}
      {%- endif %}
    {%- endfor %}

    case
      {{ error_conditions | join('\n        ') }}
      else null
    end as error_code,

    case
      {{ error_messages | join('\n        ') }}
      else null
    end as error_message

  from raw
)

select * from validated
{% endmacro %}
