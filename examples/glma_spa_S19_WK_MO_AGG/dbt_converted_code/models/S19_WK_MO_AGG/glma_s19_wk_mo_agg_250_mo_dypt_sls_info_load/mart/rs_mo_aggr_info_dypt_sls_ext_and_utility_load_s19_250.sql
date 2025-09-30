{{ config(
    materialized = 'table',
    schema=var('Redshift_gdap_Utility'),
    tags=['glma_s19_wk_mo_agg_mo_agg_grandmaster','glma_s19_wk_mo_agg_250_mo_dypt_sls_info_load'],
    post_hook="{{ mo_aggr_info_upsert_s19_250() }}",
    alias = 'MO_AGGR_INFO_DYPT_SLS'
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
)

SELECT
   CAST(DIV(DATE_DIFF(A.cal_dt, DATE '1970-01-02', DAY), 7) + 2 AS INT64) AS wk_end_thu_id_nu,
   A.MCD_GBAL_LCAT_ID_NU,
   CAST(COUNT(DISTINCT A.cal_dt) AS INT64) AS wkly_aggr_pmix_dy_cnt_qt,
   CAST(
     COALESCE(C.WK_END_THU_HLDY_DY_CNT_QT, 0) - COUNT(DISTINCT B.cal_dt)
     AS INT64
   ) AS aggr_pmix_non_rept_hldy_qt,
   0 AS wkly_aggr_sls_7dy_rule_fl,
   0 AS wkly_aggr_dypt_7dy_rule_fl,
   CAST(
     CASE WHEN (CAST(COUNT(DISTINCT A.cal_dt) AS INT64) +
                (COALESCE(C.WK_END_THU_HLDY_DY_CNT_QT, 0) - COUNT(DISTINCT B.cal_dt))) = 7
          THEN 1 ELSE 0 END
     AS INT64
   ) AS wkly_aggr_pmix_7dy_rule_fl,
   P.updt_dw_audt_ts AS updt_dw_audt_ts
FROM {{ var('Redshift_gdap_Schema') }}.dly_pmix A
LEFT JOIN {{ var('Redshift_gdap_Schema') }}.ctry_hldy B
  ON B.CTRY_ISO_NU = A.TERR_CD
 AND B.cal_dt = A.cal_dt
 AND B.XPCT_CLSD_DY_FL = 1
LEFT JOIN cal_dt_weekly_hldy C
  ON C.WK_END_THU_ID_NU = CAST(DIV(DATE_DIFF(A.cal_dt, DATE '1970-01-02', DAY), 7) + 2 AS INT64)
 AND C.CTRY_ISO_NU = A.TERR_CD
JOIN parms P
  ON A.cal_dt BETWEEN P.wk_from_dt AND P.wk_to_dt
WHERE A.TERR_CD IN (840)
GROUP BY
   1, A.MCD_GBAL_LCAT_ID_NU, C.WK_END_THU_HLDY_DY_CNT_QT, P.updt_dw_audt_ts
