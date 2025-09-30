-- macros/load_mo_pmix.sql
{% macro insert_mo_pmix() %}

INSERT INTO `{{ var('Redshift_gdap_Schema') }}.mo_pmix` (
    YR_NU,
    MO_NU,
    MO_PERD_NU,
    MCD_GBAL_LCAT_ID_NU,
    TERR_CD,
    SLD_MENU_ITM_ID,
    PMIX_UNT_TYP_ID_NU,
    CURN_ISO_NU,
    MO_PMIX_NET_PRC_AM,
    MO_CMBO_UP_DWN_AM,
    MO_POS_KEY_NET_PRC_AM,
    FOOD_COST_AM,
    PAPR_COST_AM,
    OTH_COST_AM,
    TOT_COST_AM,
    MO_POS_KEY_QT,
    MO_CMBO_UNTS_SLD_QT,
    MO_TOT_SLD_QT,
    MO_PMIX_DY_CNT_QT,
    MO_ITM_CMBO_UP_DWN_AM
)
SELECT
    YR_NU,
    MO_NU,
    MO_PERD_NU,
    MCD_GBAL_LCAT_ID_NU,
    TERR_CD,
    SLD_MENU_ITM_ID,
    PMIX_UNT_TYP_ID_NU,
    CURN_ISO_NU,
    MO_PMIX_NET_PRC_AM,
    MO_CMBO_UP_DWN_AM,
    MO_POS_KEY_NET_PRC_AM,
    FOOD_COST_AM,
    PAPR_COST_AM,
    OTH_COST_AM,
    TOT_COST_AM,
    MO_POS_KEY_QT,
    MO_CMBO_UNTS_SLD_QT,
    MO_TOT_SLD_QT,
    MO_PMIX_DY_CNT_QT,
    MO_ITM_CMBO_UP_DWN_AM
FROM `{{ var('Redshift_gdap_Utility') }}.mo_pmix`;

{% endmacro %}
