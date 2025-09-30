{% macro get_schema_rules_for_validation(table_name) %}
  {% set metadata_relation = source('schema_compliance_check','schema_rules') %}

  {{ log("Getting schema rules for table: " ~ table_name, info=True) }}

  {% set query %}
    select
      column_name,
      expected_type,
      nullable,
      max_length,
      format,
      precision
    from {{ metadata_relation }}
    where table_name = '{{ table_name }}'
  {% endset %}

  {{ log("Prepared query:\n" ~ query, info=True) }}
  {% set results = dbt_utils.get_query_results_as_dict("select * from " ~ ref('stg_schema_rules') ~ " where table_name = '" ~ table_name ~ "'") %}
  {{ log("results : " ~ results, info=True) }}
  {% if execute %}
    {% set results = dbt_utils.get_query_results_as_dict("select * from " ~ ref('stg_schema_rules') ~ " where table_name = '" ~ table_name ~ "'") %}
    {% if results %}
      {% set rules = dbt_utils.get_query_results_as_dict(results) %}
      {{ log("Fetched " ~ rules | length ~ " rules", info=True) }}
      {{ log("Rules (sample): " ~ rules[:3] | tojson, info=True) }}
      {{ return(results) }}
    {% else %}
      {{ log("No results returned for schema rules", info=True) }}
      {{ return([]) }}
    {% endif %}
  {% else %}
    {{ log("Not executing, skipping query", info=True) }}
    {{ return(results) }}
  {% endif %}
{% endmacro %}
