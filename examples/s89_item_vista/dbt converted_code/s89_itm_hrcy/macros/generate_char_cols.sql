-- macros/generate_char_cols.sql

{% macro generate_char_cols(terr_cd_param) %}

  {% set get_chars_query %}
    SELECT
      SURR_ITM_CHAR_ID,
      ITM_CHAR_NA
    FROM
      {{ source('Oracle_IM_Schema', 'itm_char') }} {# Ensure 'Oracle_IM_Schema' is your correctly defined source name #}
    WHERE
      terr_cd = '{{ terr_cd_param }}'
  {% endset %} {# No semicolon here, this is correct for Jinja #}

  {% set results = run_query(get_chars_query) %}

  {% if execute %}
    {% set lcl_chars_query_parts = [] %}
    {% for row in results %}
      {% set surr_itm_char_id = row.SURR_ITM_CHAR_ID %}
      {% set itm_char_na = row.ITM_CHAR_NA %}
      {% set column_name = itm_char_na | replace(' ', '_') %}
      {% set temp_query_part = "COALESCE(MAX(CASE WHEN CHAR_VAL.SURR_ITM_CHAR_ID = " ~ surr_itm_char_id ~ " THEN VLD_LIST.ITM_CHAR_VLD_LIST_DS ELSE NULL END), '') AS " ~ column_name %}
      {% do lcl_chars_query_parts.append(temp_query_part) %}
    {% endfor %}
    {{ lcl_chars_query_parts | join(',\n  ') }}
  {% else %}
    -- Return a placeholder if not in execute mode (e.g., during parsing or dbt build without --target dev)
    1 as placeholder_char_col
  {% endif %}

{% endmacro %}