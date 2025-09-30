{% macro delete_wkly_dypt_sls() %}


  {% set date_query %}
    WITH date_context AS (
      SELECT
        WK_FROM_DT,
        WK_TO_DT
      FROM {{ source('RMDW_RAW', 'JS_IN_PARMS_AND_MAX_CALDT_psv') }}
      QUALIFY ROW_NUMBER() OVER (ORDER BY WK_FROM_DT DESC) = 1
    )
    SELECT
      FORMAT_DATE('%Y-%m-%d', WK_FROM_DT) AS WK_FROM_DT,
      FORMAT_DATE('%Y-%m-%d', WK_TO_DT) AS WK_TO_DT
    FROM date_context
  {% endset %}

  {% set date_range = run_query(date_query) %}
  {% if execute %}
      {% set wk_from_dt = date_range.columns[0].values()[0] %}
      {% set wk_to_dt   = date_range.columns[1].values()[0] %}
  {% else %}
      {% set wk_from_dt = '1970-01-01' %}
      {% set wk_to_dt   = '1970-01-01' %}
  {% endif %}

  {# --- Perform DELETE on the target table --- #}
  {{ log("Deleting from " ~ var('Redshift_gdap_Schema') ~ ".WKLY_DYPT_SLS_WK_END_THU between " ~ wk_from_dt ~ " and " ~ wk_to_dt, info=True) }}

  DELETE FROM {{ var('Redshift_gdap_Schema') }}.WKLY_DYPT_SLS_WK_END_THU
  WHERE WK_END_THU_END_DT BETWEEN DATE('{{ wk_from_dt }}') AND DATE('{{ wk_to_dt }}');

{% endmacro %}
