{{ config(
    materialized='table',
    schema=var('Redshift_gdap_Utility'),
    tags=['glma_s19_wk_mo_agg_mo_agg_grandmaster','glma_s19_wk_mo_agg_230_mo_tm_seg_sls_info_load'],
    post_hook="{{ mo_aggr_info_tm_seg_sls_s19_230() }}",
    alias='MO_AGGR_INFO_TM_SEG_SLS'
) }}

WITH parms AS (
    SELECT
        DATE(mo_from_dt) AS mo_from_dt,
        DATE(mo_to_dt)   AS mo_to_dt
    FROM {{ source('RMDW_RAW', 'JS_IN_PARMS_AND_MAX_CALDT_psv') }}
    LIMIT 1
),
tm_seg_aggr AS (
    SELECT
        EXTRACT(YEAR FROM a.cal_dt) AS yr_nu,
        EXTRACT(MONTH FROM a.cal_dt) AS mo_nu,
        a.mcd_gbal_lcat_id_nu,
        CAST(SUM(a.net_sls_am) AS NUMERIC) AS mo_tm_seg_net_sls_am,
        b.rept_lc_sls_am,
        CAST(COUNT(DISTINCT a.cal_dt) AS INT64) AS mo_aggr_tm_seg_sls_dy_qt
    FROM {{ var('Redshift_gdap_Schema') }}.dy_tm_seg_sls a
    INNER JOIN {{ var('Redshift_gdap_Schema') }}.gbl_rest_rept_mo_sls b
        ON b.mcd_gbal_lcat_id_nu = a.mcd_gbal_lcat_id_nu
       AND b.yr_nu = EXTRACT(YEAR FROM a.cal_dt)
       AND b.mo_nu = EXTRACT(MONTH FROM a.cal_dt)
    CROSS JOIN parms
    WHERE a.cal_dt BETWEEN parms.mo_from_dt AND parms.mo_to_dt
      AND a.sale_typ_id_nu = 1
    GROUP BY 1, 2, a.mcd_gbal_lcat_id_nu, b.rept_lc_sls_am
)

SELECT
    yr_nu,
    mo_nu,
    mcd_gbal_lcat_id_nu,
    CASE
        WHEN rept_lc_sls_am = 0 THEN 100
        ELSE CAST((rept_lc_sls_am - mo_tm_seg_net_sls_am) / CAST(rept_lc_sls_am AS NUMERIC) * 100 AS NUMERIC)
    END AS mo_tm_seg_sls_vrnc_pc,
    mo_aggr_tm_seg_sls_dy_qt
FROM tm_seg_aggr
