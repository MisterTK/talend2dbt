{% macro insert_wkly_dypt_sls_wk_end_thu() %}
    INSERT INTO {{ var('Redshift_gdap_Schema') }}.WKLY_DYPT_SLS_WK_END_THU (
          WK_END_THU_ID_NU,
          WK_END_THU_END_DT,
          MCD_GBAL_LCAT_ID_NU,
          SALE_TYP_ID_NU,
          CURN_ISO_NU,
          DYPT_ID_NU,
          WKLY_DYPT_NET_SLS_AM,
          WKLY_DYPT_TRN_CNT_QT,
          DYPT_SLS_DY_CNT_QT,
          WKLY_DYPT_NON_REPT_HLDY_QT,
          WKLY_AGGR_DYPT_7DY_RULE_FL
    )
    SELECT
          WK_END_THU_ID_NU,
          WK_END_THU_END_DT,
          MCD_GBAL_LCAT_ID_NU,
          SALE_TYP_ID_NU,
          CURN_ISO_NU,
          DYPT_ID_NU,
          WKLY_DYPT_NET_SLS_AM,
          WKLY_DYPT_TRN_CNT_QT,
          DYPT_SLS_DY_CNT_QT,
          WKLY_DYPT_NON_REPT_HLDY_QT,
          WKLY_AGGR_DYPT_7DY_RULE_FL
    FROM {{ var('Redshift_gdap_Utility') }}.WKLY_DYPT_SLS_WK_END_THU
{% endmacro %}
