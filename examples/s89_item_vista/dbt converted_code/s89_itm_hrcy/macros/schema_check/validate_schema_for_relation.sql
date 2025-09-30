{% macro validate_schema_for_relation(source_relation, table_name) %}
    {%- set rules_sql -%}
    select * from {{ source('schema_compliance_check', 'schema_rules') }}
    where table_name = '{{ table_name }}'
    {%- endset -%}

    {%- set rules_dict = dbt_utils.get_query_results_as_dict(rules_sql) -%}

    {%- set validations = [] -%}
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
        --{% do log("Rule: " ~ rule, info=True) %}
    {% endfor %}
    {%- endif -%}

    {%- for rule in rules %}
        {%- set col = adapter.quote(rule.column_name) %}
        {%- set val = "cast(" ~ col ~ " as string)" %}

        {# Nullability check #}
        {% if rule.nullable is not none %}
            {% do validations.append(validate_nullability(col, rule.nullable, val)) %}
        {% endif %}

        {# Length check #}
        {% if rule.max_length is not none %}
            {% do validations.append(validate_length(col, val, rule.max_length)) %}
        {% endif %}

        {# Data type check #}
        {% if rule.expected_type is not none %}
            {% if rule.expected_type.upper() == 'INTEGER' %}
                {% do validations.append(validate_integer_type(col, val)) %}
            {% elif rule.expected_type.upper() == 'DATE' %}
                {% do validations.append(validate_date_type(col, val)) %}
            {% elif rule.expected_type.upper() == 'TIMESTAMP' %}
                {% do validations.append(validate_timestamp_type(col, val)) %}
            {% endif %}
        {% endif %}

    {% endfor %}
    --{{ log("validations:\n" ~ validations, info=True) }}
    {# If no validations, return default OK struct #}
    {% if validations | length == 0 %}
        {% set err_msg = '0' %}
        {% set err_code = 'OK' %}
        {{ return("NULL AS error_code, 'OK' AS error_message") }}
    {% else %}
        {% set err_msg = aggregate_row_errors(validations) %}
        {% set err_code = determine_error_code(err_msg) %}
        {{ return(err_code ~ " AS error_code, " ~ err_msg ~ " AS error_message") }}
    {% endif %}
{% endmacro %}
