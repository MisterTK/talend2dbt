{% macro delete_and_insert_wkly_tm_seg(Redshift_gdap_Utility) %}

  {# --- Get the date range from source table --- #}
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
  {{ log("Deleting from " ~ var('Redshift_gdap_Utility') ~ ".WKLY_TM_SEG_SLS_WK_END_THU between " ~ wk_from_dt ~ " and " ~ wk_to_dt, info=True) }}
  DELETE FROM {{ var('Redshift_gdap_Utility') }}.WKLY_TM_SEG_SLS_WK_END_THU
  WHERE WK_END_THU_ID_NU IN (
      SELECT DISTINCT CAST(DIV(DATE_DIFF(cal_dt, DATE '1970-01-02',day), 7) + 2 AS INT64)
      FROM {{ var('Redshift_gdap_Schema') }}.cal_dt c
      WHERE c.cal_dt BETWEEN DATE('{{ wk_from_dt }}') AND DATE('{{ wk_to_dt }}')
  );

  {# --- Perform INSERT from source to target --- #}
  INSERT INTO {{ var('Redshift_gdap_Utility') }}.WKLY_TM_SEG_SLS_WK_END_THU (
      WK_END_THU_ID_NU,
      MCD_GBAL_LCAT_ID_NU,
      TM_SEG_ID_NU,
      SALE_TYP_ID_NU,
      CURN_ISO_NU,
      WKLY_TM_SEG_NET_SLS_AM,
      WKLY_TM_SEG_TRN_CNT_QT,
      TM_SEG_SLS_DY_CNT_QT,
      WKLY_TM_NON_REPT_HLDY_QT
  )
  SELECT
      CAST(DIV(DATE_DIFF(a.cal_dt, DATE '1970-01-02', day), 7) + 2 AS INT64) AS WK_END_THU_ID_NU,
      CAST(a.MCD_GBAL_LCAT_ID_NU AS INT64),
      a.TM_SEG_ID_NU,
      a.SALE_TYP_ID_NU,
      CAST(a.CURN_ISO_NU AS int64),
      CAST(SUM(a.NET_SLS_AM) AS NUMERIC) AS WKLY_TM_SEG_NET_SLS_AM,
      CAST(SUM(a.TRN_CNT_QT) AS INT64) AS WKLY_TM_SEG_TRN_CNT_QT,
      CAST(COUNT(*) AS INT64) AS TM_SEG_SLS_DY_CNT_QT,
      CAST(
        COALESCE(c.WK_END_THU_HLDY_DY_CNT_QT, 0) - SUM(CASE WHEN b.cal_dt IS NULL THEN 0 ELSE 1 END)
      AS INT64) AS WKLY_TM_NON_REPT_HLDY_QT
  FROM {{ var('Redshift_gdap_Schema') }}.dy_tm_seg_sls a
  INNER JOIN {{ var('Redshift_gdap_Schema') }}.mcd_gbal_busn_lcat a1
      ON a1.MCD_GBAL_LCAT_ID_NU = a.MCD_GBAL_LCAT_ID_NU
  LEFT JOIN {{ var('Redshift_gdap_Schema') }}.ctry_hldy b
      ON b.CTRY_ISO_NU = a1.CTRY_ISO_NU
     AND b.cal_dt = a.cal_dt
  LEFT JOIN (
      SELECT
          CAST(DIV(DATE_DIFF(a.cal_dt, DATE '1970-01-02',day), 7) + 2 AS INT64) AS WK_END_THU_ID_NU,
          b.CTRY_ISO_NU,
          COUNT(*) AS WK_END_THU_HLDY_DY_CNT_QT
      FROM {{ var('Redshift_gdap_Schema') }}.cal_dt a
      INNER JOIN {{ var('Redshift_gdap_Schema') }}.ctry_hldy b
          ON b.cal_dt = a.cal_dt
      GROUP BY 1,2
  ) c
      ON c.WK_END_THU_ID_NU = CAST(DIV(DATE_DIFF(a.cal_dt, DATE '1970-01-02',day), 7) + 2 AS INT64)
     AND c.CTRY_ISO_NU = a1.CTRY_ISO_NU
  WHERE a.cal_dt BETWEEN DATE('{{ wk_from_dt }}') AND DATE('{{ wk_to_dt }}')
    AND a1.CTRY_ISO_NU = 840
  GROUP BY
      1, a.MCD_GBAL_LCAT_ID_NU, a.TM_SEG_ID_NU, a.SALE_TYP_ID_NU, a.CURN_ISO_NU,
      c.WK_END_THU_HLDY_DY_CNT_QT;

{% endmacro %}
