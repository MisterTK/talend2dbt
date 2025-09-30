{{ config(
    materialized='table',
    schema=var('Redshift_gdap_Utility'),
    post_hook="{{ update_part2() }}",
    tags=['glma_s19_wk_mo_agg_wk_agg_grandmaster','glma_s19_wk_mo_agg_120_wk_pmix_info_load'],
    alias='WKLY_AGGR_INFO_PMIX'
) }}

WITH parms AS (
    SELECT
        DATE(wk_from_dt) AS wk_from_dt,
        DATE(wk_to_dt) AS wk_to_dt,
        CAST(CURRENT_TIMESTAMP() AS DATETIME) AS UPDT_DW_AUDT_TS
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
)

SELECT
    CAST((DATE_DIFF(A.cal_dt, DATE('1970-01-02'), DAY) / 7) + 2 AS INT64) AS WK_END_THU_ID_NU,
    A.MCD_GBAL_LCAT_ID_NU,
    CAST(COUNT(DISTINCT A.cal_dt) AS INT64) AS WKLY_AGGR_PMIX_DY_CNT_QT,
    CAST(
        MAX(COALESCE(C.WK_END_THU_HLDY_DY_CNT_QT, 0))
        - COUNT(DISTINCT B.cal_dt) AS INT64
    ) AS AGGR_PMIX_NON_REPT_HLDY_QT,
    0 AS WKLY_AGGR_SLS_7DY_RULE_FL,
    0 AS WKLY_AGGR_DYPT_7DY_RULE_FL,
    CAST(
      CASE WHEN
        (COUNT(DISTINCT A.cal_dt) + MAX(COALESCE(C.WK_END_THU_HLDY_DY_CNT_QT, 0)) - COUNT(DISTINCT B.cal_dt)) = 7
      THEN 1 ELSE 0 END AS INT64
    ) AS WKLY_AGGR_PMIX_7DY_RULE_FL
FROM {{ var('Redshift_gdap_Schema') }}.dly_pmix A
LEFT JOIN {{ var('Redshift_gdap_Schema') }}.ctry_hldy B
  ON B.CTRY_ISO_NU = A.TERR_CD
  AND B.cal_dt = A.cal_dt
  AND B.XPCT_CLSD_DY_FL = 1
LEFT JOIN cal_dt_weekly_hldy C
  ON C.WK_END_THU_ID_NU = CAST((DATE_DIFF(A.cal_dt, DATE('1970-01-02'), DAY) / 7) + 2 AS INT64)
  AND C.CTRY_ISO_NU = A.TERR_CD
CROSS JOIN parms
WHERE A.cal_dt BETWEEN parms.wk_from_dt AND parms.wk_to_dt
  AND A.TERR_CD IN (840)
GROUP BY 1, 2
