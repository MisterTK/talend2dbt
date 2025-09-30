{% macro delete_wkly_tm_seg_sls(Redshift_gdap_Schema) %}

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
    {% set wk_to_dt = date_range.columns[1].values()[0] %}
  {% else %}
    {% set wk_from_dt = '1970-01-01' %}
    {% set wk_to_dt = '1970-01-01' %}
  {% endif %}

  {{ log("Deleting from " ~ Redshift_gdap_Schema ~ ".WKLY_TM_SEG_SLS_WK_END_THU between " ~ wk_from_dt ~ " and " ~ wk_to_dt, info=True) }}

  delete from {{ Redshift_gdap_Schema }}.WKLY_TM_SEG_SLS_WK_END_THU
  where WK_END_THU_ID_NU in (
    select distinct WK_END_THU_ID_NU
    from {{ Redshift_gdap_Schema }}.cal_dt c
    where c.cal_dt between date('{{ wk_from_dt }}') and date('{{ wk_to_dt }}')
  );

{% endmacro %}
