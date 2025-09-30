{{ config(
    materialized='table',
    schema=var('Redshift_gdap_Utility'),
    tags=['glma_s19_wk_mo_agg_wk_agg_grandmaster','glma_s19_wk_mo_agg_180_wk_sls_info_load'],
    alias='WKLY_AGGR_INFO_SLS'
) }}

WITH parms AS (
    SELECT
        DATE(wk_from_dt) AS wk_from_dt,
        DATE(wk_to_dt) AS wk_to_dt,
        CAST(CURRENT_TIMESTAMP() AS DATETIME) AS updt_dw_audt_ts
    FROM {{ source('RMDW_RAW', 'JS_IN_PARMS_AND_MAX_CALDT_psv') }}
    LIMIT 1
),

cal_dt_weekly_hldy AS (
    SELECT
        A.WK_END_THU_ID_NU,
        B.CTRY_ISO_NU,
        COUNT(*) AS WK_END_THU_HLDY_DY_CNT_QT
    FROM {{ var('Redshift_gdap_Schema') }}.cal_dt A
    INNER JOIN {{ var('Redshift_gdap_Schema') }}.ctry_hldy B
      ON B.cal_dt = A.cal_dt
    WHERE B.XPCT_CLSD_DY_FL = 1
    GROUP BY 1, 2
),

dly_sls_with_hldy AS (
    SELECT
        A.cal_dt,
        A.MCD_GBAL_LCAT_ID_NU,
        A.SALE_TYP_ID_NU,
        A1.CTRY_ISO_NU AS country,
        CAST((DATE_DIFF(A.cal_dt, DATE('1970-01-02'), DAY) / 7) + 2 AS INT64) AS WK_END_THU_ID_NU,
        B.cal_dt AS hldy_dt,
        C.WK_END_THU_HLDY_DY_CNT_QT
    FROM {{ var('Redshift_gdap_Schema') }}.dly_sls A
    INNER JOIN {{ var('Redshift_gdap_Schema') }}.mcd_gbal_busn_lcat A1
      ON A1.MCD_GBAL_LCAT_ID_NU = A.MCD_GBAL_LCAT_ID_NU
    LEFT JOIN {{ var('Redshift_gdap_Schema') }}.ctry_hldy B
      ON B.CTRY_ISO_NU = A1.CTRY_ISO_NU
     AND B.cal_dt = A.cal_dt
     AND B.XPCT_CLSD_DY_FL = 1
    LEFT JOIN cal_dt_weekly_hldy C
      ON C.WK_END_THU_ID_NU = CAST((DATE_DIFF(A.cal_dt, DATE('1970-01-02'), DAY) / 7) + 2 AS INT64)
     AND C.CTRY_ISO_NU = A1.CTRY_ISO_NU
    CROSS JOIN parms
    WHERE A.cal_dt BETWEEN parms.wk_from_dt AND parms.wk_to_dt
      AND A1.CTRY_ISO_NU = 840
      AND A.SALE_TYP_ID_NU = 1
)

SELECT
    WK_END_THU_ID_NU,
    MCD_GBAL_LCAT_ID_NU,
    CAST(COUNT(DISTINCT cal_dt) AS INT64) AS WKLY_AGGR_SLS_DY_CNT_QT,
    CAST(
        COALESCE(MAX(WK_END_THU_HLDY_DY_CNT_QT), 0) - COUNT(DISTINCT hldy_dt) AS INT64
    ) AS AGGR_SLS_NON_REPT_HLDY_QT,
    0 AS wkly_aggr_pmix_7dy_rule_fl,
    0 AS wkly_aggr_dypt_7dy_rule_fl,
    CAST(
        CASE WHEN (COUNT(DISTINCT cal_dt) + COALESCE(MAX(WK_END_THU_HLDY_DY_CNT_QT), 0) - COUNT(DISTINCT hldy_dt)) = 7
        THEN 1 ELSE 0 END AS INT64
    ) AS WKLY_AGGR_SLS_7DY_RULE_FL
FROM dly_sls_with_hldy
GROUP BY 1, 2
