{{ config(
    materialized='incremental',
    schema=var('Redshift_gdap_Stage'),
    alias='WKLY_PMIX_WK_END_THU_STGE',
    incremental_strategy='merge',
    unique_key=[
        'WK_END_THU_ID_NU',
        'MCD_GBAL_LCAT_ID_NU',
        'TERR_CD',
        'SLD_MENU_ITM_ID',
        'PMIX_UNT_TYP_ID_NU'
    ],
    tags=['glma_s19_wk_mo_agg_wk_agg_grandmaster','glma_s19_wk_mo_agg_130_wk_pmix_load']
) }}



WITH date_context AS (
    SELECT
        FORMAT_DATE('%Y-%m-%d', CAST(WK_FROM_DT AS DATE)) AS WK_FROM_DT,
        FORMAT_DATE('%Y-%m-%d', CAST(WK_TO_DT AS DATE))   AS WK_TO_DT
    FROM {{ source('RMDW_RAW', 'JS_IN_PARMS_AND_MAX_CALDT_psv') }}
    QUALIFY ROW_NUMBER() OVER (ORDER BY WK_FROM_DT DESC) = 1
),

base_pm AS (
SELECT
    CAST(DIV(DATE_DIFF(pm.cal_dt , DATE '1970-01-02',DAY), 7) + 2 AS INT64) AS WK_END_THU_ID_NU,
    pm.*
FROM {{ var("Redshift_gdap_Schema") }}.dly_pmix pm

    CROSS JOIN date_context d
    WHERE pm.cal_dt BETWEEN DATE(d.WK_FROM_DT) AND DATE(d.WK_TO_DT)
      AND pm.TERR_CD = 840
),


agg_costs AS (
    SELECT
        WK_END_THU_ID_NU,
        MCD_GBAL_LCAT_ID_NU,
        TERR_CD,
        SLD_MENU_ITM_ID,
        PMIX_UNT_TYP_ID_NU,
        DLY_PMIX_NET_PRC_AM,
        COALESCE(CMBO_UP_DWN_AM, 0) AS CMBO_UP_DWN_AM,
        COALESCE(POS_KEY_NET_PRC_AM, 0) AS POS_KEY_NET_PRC_AM,
        CASE WHEN SUM(TOT_SLD_QT) > 0 AND SUM(food_cost_am) > 0
             THEN SUM(CASE WHEN food_cost_am > 0 THEN food_cost_am * TOT_SLD_QT ELSE 0 END)
                  / SUM(CASE WHEN food_cost_am > 0 THEN TOT_SLD_QT ELSE 0 END)
             ELSE 0 END AS FOOD_COST_AM,
        CASE WHEN SUM(TOT_SLD_QT) > 0 AND SUM(PAPR_COST_AM) > 0
             THEN SUM(CASE WHEN PAPR_COST_AM > 0 THEN PAPR_COST_AM * TOT_SLD_QT ELSE 0 END)
                  / SUM(CASE WHEN PAPR_COST_AM > 0 THEN TOT_SLD_QT ELSE 0 END)
             ELSE 0 END AS PAPR_COST_AM,
        CASE WHEN SUM(TOT_SLD_QT) > 0 AND SUM(OTH_COST_AM) > 0
             THEN SUM(CASE WHEN OTH_COST_AM > 0 THEN OTH_COST_AM * TOT_SLD_QT ELSE 0 END)
                  / SUM(CASE WHEN OTH_COST_AM > 0 THEN TOT_SLD_QT ELSE 0 END)
             ELSE 0 END AS OTH_COST_AM,
        CASE WHEN SUM(TOT_SLD_QT) > 0 AND SUM(TOT_COST_AM) > 0
             THEN SUM(CASE WHEN TOT_COST_AM > 0 THEN TOT_COST_AM * TOT_SLD_QT ELSE 0 END)
                  / SUM(CASE WHEN TOT_COST_AM > 0 THEN TOT_SLD_QT ELSE 0 END)
             ELSE 0 END AS TOT_COST_AM,
        COALESCE(ITM_CMBO_UP_DWN_AM, 0) AS ITM_CMBO_UP_DWN_AM
    FROM base_pm
    GROUP BY
        WK_END_THU_ID_NU, MCD_GBAL_LCAT_ID_NU, TERR_CD, SLD_MENU_ITM_ID,
        PMIX_UNT_TYP_ID_NU, DLY_PMIX_NET_PRC_AM, CMBO_UP_DWN_AM,
        POS_KEY_NET_PRC_AM, ITM_CMBO_UP_DWN_AM
),

holidays_cte AS (
SELECT
    a.WK_END_THU_ID_NU,
    b.CTRY_ISO_NU,
    COUNT(*) AS WK_END_THU_HLDY_DY_CNT_QT
FROM (
    SELECT
        CAST(
            DIV(DATE_DIFF(c.cal_dt , DATE '1970-01-02',DAY), 7) + 2 AS INT64
        ) AS WK_END_THU_ID_NU,
        cal_dt
    FROM {{ var("Redshift_gdap_Schema") }}.cal_dt c

    ) a
    INNER JOIN {{ var("Redshift_gdap_Schema") }}.ctry_hldy b
        ON b.cal_dt = a.cal_dt
    WHERE b.XPCT_CLSD_DY_FL = 1
    GROUP BY a.WK_END_THU_ID_NU, b.CTRY_ISO_NU
),

DP AS (
    SELECT
        pm.WK_END_THU_ID_NU,
        pm.MCD_GBAL_LCAT_ID_NU,
        pm.TERR_CD,
        pm.SLD_MENU_ITM_ID,
        pm.PMIX_UNT_TYP_ID_NU,
        pm.CURN_ISO_NU,
        pm.DLY_PMIX_NET_PRC_AM AS WKLY_PMIX_PRC_AM,
        pm.CMBO_UP_DWN_AM AS WKLY_CMBO_UP_DWN_AM,
        pm.POS_KEY_NET_PRC_AM AS WKLY_POS_KEY_NET_PRC_AM,
        ac.FOOD_COST_AM,
        ac.PAPR_COST_AM,
        ac.OTH_COST_AM,
        ac.TOT_COST_AM,
        SUM(pm.POS_KEY_QT) AS WKLY_POS_KEY_QT,
        SUM(pm.CMBO_UNTS_SLD_QT) AS WKLY_CMBO_UNTS_SLD_QT,
        SUM(pm.TOT_SLD_QT) AS WKLY_TOT_SLD_QT,
        CAST(COUNT(DISTINCT pm.cal_dt) AS INT64) AS PMIX_DY_CNT_QT,
        CAST(COALESCE(h.WK_END_THU_HLDY_DY_CNT_QT, 0)
             - SUM(CASE WHEN ch.cal_dt IS NULL THEN 0 ELSE 1 END)
        AS INT64) AS PMIX_NON_REPT_HLDY_QT,
        t.WK_END_THU_END_DT,
        s.MENU_ITM_CMBO_FL,
        i.WKLY_AGGR_PMIX_7DY_RULE_FL,
        pm.ITM_CMBO_UP_DWN_AM AS WKLY_ITM_CMBO_UP_DWN_AM
    FROM base_pm pm
    INNER JOIN agg_costs ac
        ON pm.WK_END_THU_ID_NU = ac.WK_END_THU_ID_NU
       AND pm.MCD_GBAL_LCAT_ID_NU = ac.MCD_GBAL_LCAT_ID_NU
       AND pm.TERR_CD = ac.TERR_CD
       AND pm.SLD_MENU_ITM_ID = ac.SLD_MENU_ITM_ID
       AND pm.PMIX_UNT_TYP_ID_NU = ac.PMIX_UNT_TYP_ID_NU
       AND pm.DLY_PMIX_NET_PRC_AM = ac.DLY_PMIX_NET_PRC_AM
       AND COALESCE(pm.CMBO_UP_DWN_AM, 0) = ac.CMBO_UP_DWN_AM
       AND COALESCE(pm.POS_KEY_NET_PRC_AM, 0) = ac.POS_KEY_NET_PRC_AM
       AND COALESCE(pm.ITM_CMBO_UP_DWN_AM, 0) = ac.ITM_CMBO_UP_DWN_AM
    LEFT JOIN {{ var("Redshift_gdap_Schema") }}.ctry_hldy ch
        ON ch.CTRY_ISO_NU = pm.TERR_CD
       AND ch.cal_dt = pm.cal_dt
       AND ch.XPCT_CLSD_DY_FL = 1
    LEFT JOIN holidays_cte h
        ON h.WK_END_THU_ID_NU = pm.WK_END_THU_ID_NU
       AND h.CTRY_ISO_NU = pm.TERR_CD
    INNER JOIN {{ var("Redshift_gdap_Schema") }}.wk_end_thu t
        ON t.WK_END_THU_ID_NU = pm.WK_END_THU_ID_NU
    INNER JOIN {{ var("Redshift_gdap_Schema") }}.menu_itm_ds s
        ON pm.SLD_MENU_ITM_ID = s.SLD_MENU_ITM_ID
       AND pm.TERR_CD = s.TERR_CD
       AND s.MENU_ITM_END_DT IS NULL
    INNER JOIN {{ var("Redshift_gdap_Schema") }}.WKLY_AGGR_INFO i
        ON i.WK_END_THU_ID_NU = pm.WK_END_THU_ID_NU
       AND i.MCD_GBAL_LCAT_ID_NU = pm.MCD_GBAL_LCAT_ID_NU
    GROUP BY
        pm.WK_END_THU_ID_NU, pm.MCD_GBAL_LCAT_ID_NU, pm.TERR_CD, pm.SLD_MENU_ITM_ID,
        pm.PMIX_UNT_TYP_ID_NU, pm.CURN_ISO_NU, pm.DLY_PMIX_NET_PRC_AM, pm.CMBO_UP_DWN_AM,
        pm.POS_KEY_NET_PRC_AM, ac.FOOD_COST_AM, ac.PAPR_COST_AM, ac.OTH_COST_AM, ac.TOT_COST_AM,
        h.WK_END_THU_HLDY_DY_CNT_QT, t.WK_END_THU_END_DT, s.MENU_ITM_CMBO_FL,
        i.WKLY_AGGR_PMIX_7DY_RULE_FL, pm.ITM_CMBO_UP_DWN_AM
)

SELECT
    DP.* EXCEPT(WKLY_PMIX_PRC_AM, WKLY_CMBO_UP_DWN_AM, WKLY_POS_KEY_NET_PRC_AM),
    CAST(DP.WKLY_PMIX_PRC_AM AS NUMERIC) AS WKLY_PMIX_PRC_AM,
    CAST(DP.WKLY_CMBO_UP_DWN_AM AS NUMERIC) AS WKLY_CMBO_UP_DWN_AM,
    CAST(DP.WKLY_POS_KEY_NET_PRC_AM AS NUMERIC) AS WKLY_POS_KEY_PRC_AM,
    CASE WHEN DP.WKLY_POS_KEY_NET_PRC_AM > 0 THEN DP.WKLY_POS_KEY_QT ELSE 0 END AS WKLY_CNSM_PRC_SLD_QT,
    CAST(DP.WKLY_PMIX_PRC_AM *
         CASE WHEN DP.WKLY_POS_KEY_NET_PRC_AM > 0 THEN DP.WKLY_POS_KEY_QT ELSE 0 END
    AS NUMERIC) AS WKLY_CNSM_PRC_XTND_AM,
    CASE
        WHEN DP.WKLY_POS_KEY_NET_PRC_AM > 0 AND DP.MENU_ITM_CMBO_FL = 1 THEN DP.WKLY_POS_KEY_QT
        WHEN DP.WKLY_POS_KEY_NET_PRC_AM > 0 AND DP.MENU_ITM_CMBO_FL = 0 THEN DP.WKLY_TOT_SLD_QT
        ELSE 0
    END AS WKLY_SLS_ANAL_SLD_QT,
    CAST(
        CASE
            WHEN DP.WKLY_POS_KEY_NET_PRC_AM > 0 AND DP.MENU_ITM_CMBO_FL = 1 THEN DP.WKLY_POS_KEY_QT * DP.WKLY_CMBO_UP_DWN_AM
            WHEN DP.WKLY_POS_KEY_NET_PRC_AM > 0 AND DP.MENU_ITM_CMBO_FL = 0 THEN DP.WKLY_TOT_SLD_QT * DP.WKLY_POS_KEY_NET_PRC_AM
            ELSE 0
        END
    AS NUMERIC) AS WKLY_SLS_ANAL_XTND_AM
FROM DP
