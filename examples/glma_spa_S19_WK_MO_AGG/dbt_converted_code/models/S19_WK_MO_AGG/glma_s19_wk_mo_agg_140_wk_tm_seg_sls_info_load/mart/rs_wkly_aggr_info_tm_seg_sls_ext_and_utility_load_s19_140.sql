{{ config(
    materialized='table',
    unique_key=['WK_END_THU_ID_NU','MCD_GBAL_LCAT_ID_NU'],
    schema=var('Redshift_gdap_Utility'),
    post_hook="{{ merge_wkly_aggr_info_s19_140() }}",
    tags=['glma_s19_wk_mo_agg_wk_agg_grandmaster','glma_s19_wk_mo_agg_140_wk_tm_seg_sls_info_load'],
    alias='WKLY_AGGR_INFO_TM_SEG_SLS'
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
    CAST(DIV(DATE_DIFF(a.cal_dt, DATE '1970-01-02', DAY), 7) + 2 AS INT64) AS WK_END_THU_ID_NU,
    a.MCD_GBAL_LCAT_ID_NU,
    CAST(COUNT(DISTINCT a.cal_dt) AS INT64) AS WKLY_TM_SEG_SLS_DY_CNT_QT,

    CAST(
        COALESCE(c.WK_END_THU_HLDY_DY_CNT_QT, 0) - COUNT(DISTINCT b.cal_dt) 
        AS INT64
    ) AS AGGR_TM_NON_REPT_HLDY_QT,

    CASE 
        WHEN (COUNT(DISTINCT a.cal_dt) + (COALESCE(c.WK_END_THU_HLDY_DY_CNT_QT, 0) - COUNT(DISTINCT b.cal_dt))) = 7
        THEN 'Y' ELSE 'N' 
    END AS WKLY_AGGR_TM_7DY_RULE_FL

FROM {{ var('Redshift_gdap_Schema') }}.dy_tm_seg_sls a
INNER JOIN {{ var('Redshift_gdap_Schema') }}.mcd_gbal_busn_lcat a1
    ON a1.MCD_GBAL_LCAT_ID_NU = a.MCD_GBAL_LCAT_ID_NU

LEFT JOIN {{ var('Redshift_gdap_Schema') }}.ctry_hldy b
    ON b.CTRY_ISO_NU = a1.CTRY_ISO_NU
   AND b.cal_dt = a.cal_dt
   AND b.XPCT_CLSD_DY_FL = 1  -- only expected closed days

LEFT JOIN cal_dt_weekly_hldy c
    ON c.WK_END_THU_ID_NU = DIV(DATE_DIFF(a.cal_dt, DATE '1970-01-02', DAY), 7) + 2
   AND c.CTRY_ISO_NU = a1.CTRY_ISO_NU

CROSS JOIN parms p
WHERE a.cal_dt BETWEEN p.wk_from_dt AND p.wk_to_dt
  AND a1.CTRY_ISO_NU = 840
GROUP BY 1, 2, c.WK_END_THU_HLDY_DY_CNT_QT
