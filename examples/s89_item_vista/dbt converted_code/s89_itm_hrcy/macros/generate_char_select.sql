-- macros/generate_char_select.sql
{% macro generate_char_select_clause(source_table, p_terr_cd) %}
    {% set char_query %}
        SELECT SURR_ITM_CHAR_ID, ITM_CHAR_NA
        FROM {{ source_table }}
        WHERE terr_cd = '{{ p_terr_cd }}'
    {% endset %}

    {% set results = run_query(char_query) %}

    {% if execute %}
        {% set char_columns = [] %}
        {% for row in results %}
            {% set char_id = row['SURR_ITM_CHAR_ID'] %}
            {% set char_name = row['ITM_CHAR_NA'] | replace(' ', '_') %}
            {% set column_expression = "COALESCE(MAX(CASE WHEN CHAR_VAL.SURR_ITM_CHAR_ID = '" ~ char_id ~ "' THEN VLD_LIST.ITM_CHAR_VLD_LIST_DS ELSE NULL END), '') AS " ~ char_name %}
            {% do char_columns.append(column_expression) %}
        {% endfor %}
        {{ char_columns | join(',\n') }}
    {% else %}
        -- Return a placeholder if not in execute mode (e.g., during parsing)
        1 as placeholder_column
    {% endif %}
{% endmacro %}