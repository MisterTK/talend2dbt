{% macro insert_wkly_sls_wk_end_thu() %}
  {% set sql %}
    INSERT INTO `{{ var('Redshift_gdap_Schema') }}.wkly_sls_wk_end_thu` (
      WK_END_THU_ID_NU,
      WK_END_THU_END_DT,
      MCD_GBAL_LCAT_ID_NU,
      SALE_TYP_ID_NU,
      CURN_ISO_NU,
      WKLY_NET_SLS_AM,
      WKLY_TRN_CNT_QT,
      WKLY_SLD_REDM_QT,
      WKLY_SLS_DY_CNT_QT,
      WKLY_NON_REPT_HLDY_QT,
      WKLY_AGGR_SLS_7DY_RULE_FL
    )
    SELECT
      WK_END_THU_ID_NU,
      WK_END_THU_END_DT,
      MCD_GBAL_LCAT_ID_NU,
      SALE_TYP_ID_NU,
      CURN_ISO_NU,
      WKLY_NET_SLS_AM,
      WKLY_TRN_CNT_QT,
      WKLY_SLD_REDM_QT,
      WKLY_SLS_DY_CNT_QT,
      WKLY_NON_REPT_HLDY_QT,
      WKLY_AGGR_SLS_7DY_RULE_FL
    FROM `{{ var('Redshift_gdap_Utility') }}.wkly_sls_wk_end_thu`
  {% endset %}

  {{ return(sql) }}
{% endmacro %}
