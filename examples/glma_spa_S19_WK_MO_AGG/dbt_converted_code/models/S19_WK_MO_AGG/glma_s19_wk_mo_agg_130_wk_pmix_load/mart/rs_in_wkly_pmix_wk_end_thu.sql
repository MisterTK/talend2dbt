{{
    config(
        materialized = "table",
        schema = var('Redshift_gdap_Schema'),
        tags=['glma_s19_wk_mo_agg_wk_agg_grandmaster','glma_s19_wk_mo_agg_130_wk_pmix_load'],
        alias = "WKLY_PMIX_WK_END_THU",
        pre_hook = "
            DELETE FROM {{ var('Redshift_gdap_Schema') }}.WKLY_PMIX_WK_END_THU
            WHERE WK_END_THU_END_DT BETWEEN (
                SELECT DATE(WK_FROM_DT) 
                FROM (
                    SELECT 
                        FORMAT_DATE('%Y-%m-%d', CAST(WK_FROM_DT AS DATE)) AS WK_FROM_DT,
                        FORMAT_DATE('%Y-%m-%d', CAST(WK_TO_DT AS DATE))   AS WK_TO_DT
                    FROM {{ source('RMDW_RAW', 'JS_IN_PARMS_AND_MAX_CALDT_psv') }}
                    QUALIFY ROW_NUMBER() OVER (ORDER BY WK_FROM_DT DESC) = 1
                )
            )
            AND (
                SELECT DATE(WK_TO_DT) 
                FROM (
                    SELECT 
                        FORMAT_DATE('%Y-%m-%d', CAST(WK_FROM_DT AS DATE)) AS WK_FROM_DT,
                        FORMAT_DATE('%Y-%m-%d', CAST(WK_TO_DT AS DATE))   AS WK_TO_DT
                    FROM {{ source('RMDW_RAW', 'JS_IN_PARMS_AND_MAX_CALDT_psv') }}
                    QUALIFY ROW_NUMBER() OVER (ORDER BY WK_FROM_DT DESC) = 1
                )
            )
        "
    )
}}

WITH date_context AS (
    SELECT
        FORMAT_DATE('%Y-%m-%d', CAST(WK_FROM_DT AS DATE)) AS WK_FROM_DT,
        FORMAT_DATE('%Y-%m-%d', CAST(WK_TO_DT AS DATE))   AS WK_TO_DT
    FROM {{ source('RMDW_RAW', 'JS_IN_PARMS_AND_MAX_CALDT_psv') }}
    QUALIFY ROW_NUMBER() OVER (ORDER BY WK_FROM_DT DESC) = 1
)

SELECT 
    s.WK_END_THU_ID_NU,
    s.WK_END_THU_END_DT,
    s.MCD_GBAL_LCAT_ID_NU,
    s.TERR_CD,
    s.SLD_MENU_ITM_ID,
    s.PMIX_UNT_TYP_ID_NU,
    s.CURN_ISO_NU,
    s.WKLY_PMIX_PRC_AM,
    s.WKLY_POS_KEY_QT,
    s.WKLY_CMBO_UNTS_SLD_QT,
    s.WKLY_TOT_SLD_QT,
    s.WKLY_CMBO_UP_DWN_AM,
    s.WKLY_POS_KEY_PRC_AM,
    s.PMIX_DY_CNT_QT,
    s.PMIX_NON_REPT_HLDY_QT,
    s.FOOD_COST_AM,
    s.PAPR_COST_AM,
    s.OTH_COST_AM,
    s.TOT_COST_AM,
    s.MENU_ITM_CMBO_FL,
    s.WKLY_AGGR_PMIX_7DY_RULE_FL,
    s.WKLY_CNSM_PRC_SLD_QT,
    s.WKLY_CNSM_PRC_XTND_AM,
    s.WKLY_SLS_ANAL_SLD_QT,
    s.WKLY_SLS_ANAL_XTND_AM,
    s.WKLY_ITM_CMBO_UP_DWN_AM
FROM {{ var("Redshift_gdap_Stage") }}.WKLY_PMIX_WK_END_THU_STGE s
JOIN date_context d
    ON s.WK_END_THU_END_DT BETWEEN DATE(d.WK_FROM_DT) AND DATE(d.WK_TO_DT)
