{% macro insert_wkly_tm_seg() %}

INSERT INTO {{ var('Redshift_gdap_Schema') }}.WKLY_TM_SEG_SLS_WK_END_THU (
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
    WK_END_THU_ID_NU,
    MCD_GBAL_LCAT_ID_NU,
    TM_SEG_ID_NU,
    SALE_TYP_ID_NU,
    cast(CURN_ISO_NU as string),
    WKLY_TM_SEG_NET_SLS_AM,
    WKLY_TM_SEG_TRN_CNT_QT,
    TM_SEG_SLS_DY_CNT_QT,
    WKLY_TM_NON_REPT_HLDY_QT
FROM {{ var('Redshift_gdap_Utility') }}.WKLY_TM_SEG_SLS_WK_END_THU

{% endmacro %}
