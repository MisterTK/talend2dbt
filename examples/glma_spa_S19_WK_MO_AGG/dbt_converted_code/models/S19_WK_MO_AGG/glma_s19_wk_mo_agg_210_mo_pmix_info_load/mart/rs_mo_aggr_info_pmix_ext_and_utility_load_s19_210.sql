{{ config(
    materialized='table',
    schema=var('Redshift_gdap_Utility'),
    tags=['glma_s19_wk_mo_agg_mo_agg_grandmaster','glma_s19_wk_mo_agg_210_mo_pmix_info_load'],
    alias='MO_AGGR_INFO_PMIX'
) }}

WITH parms AS (
    SELECT
        DATE(mo_from_dt) AS mo_from_dt,
        DATE(mo_to_dt)   AS mo_to_dt,
        CAST(CURRENT_TIMESTAMP() AS DATETIME) AS updt_dw_audt_ts
    FROM {{ source('RMDW_RAW','JS_IN_PARMS_AND_MAX_CALDT_psv') }}
    LIMIT 1
),
mo_agg_prep AS (
    SELECT
        EXTRACT(YEAR FROM a.cal_dt) AS yr_nu,
        EXTRACT(MONTH FROM a.cal_dt) AS mo_nu,
        a.mcd_gbal_lcat_id_nu,
        CAST(SUM(
            CASE 
                WHEN a.pmix_unt_typ_id_nu = 1 THEN a.pos_key_qt * a.dly_pmix_net_prc_am
                ELSE 0.0
            END
        ) AS NUMERIC) AS sub_tot,
        CAST(COUNT(DISTINCT a.cal_dt) AS INT64) AS mo_aggr_pmix_dy_qt,
        d.rept_lc_sls_am
    FROM {{ var('Redshift_gdap_Schema') }}.dly_pmix a
    INNER JOIN {{ var('Redshift_gdap_Schema') }}.gbl_rest_rept_mo_sls d
        ON d.mcd_gbal_lcat_id_nu = a.mcd_gbal_lcat_id_nu
        AND d.yr_nu = EXTRACT(YEAR FROM a.cal_dt)
        AND d.mo_nu = EXTRACT(MONTH FROM a.cal_dt)
        AND a.terr_cd IN (840)
    CROSS JOIN parms p
    WHERE a.cal_dt BETWEEN p.mo_from_dt AND p.mo_to_dt
      AND a.sld_menu_itm_id NOT IN (8460,8461,8492,8493,8494,8495,8496,8497,8498,8499,936,937)
    GROUP BY 1, 2, a.mcd_gbal_lcat_id_nu, d.rept_lc_sls_am
)

SELECT
    yr_nu,
    mo_nu,
    mcd_gbal_lcat_id_nu,
    CASE 
        WHEN rept_lc_sls_am = 0 THEN CAST(100 AS NUMERIC)
        ELSE CAST((rept_lc_sls_am - sub_tot) / CAST(rept_lc_sls_am AS NUMERIC) * 100 AS NUMERIC)
    END AS mo_pmix_sls_vrnc_pc,
    mo_aggr_pmix_dy_qt,
    0 AS mo_aggr_dypt_7dy_rule_fl,
    CASE 
        WHEN ABS(
            CASE 
                WHEN rept_lc_sls_am = 0 THEN CAST(100 AS NUMERIC)
                ELSE CAST((rept_lc_sls_am - sub_tot) / CAST(rept_lc_sls_am AS NUMERIC) * 100 AS NUMERIC)
            END
        ) <= 5.0 THEN 1
        ELSE 0
    END AS mo_aggr_pmix_7dy_rule_fl,
    CURRENT_DATETIME() AS updt_dw_audt_ts
FROM mo_agg_prep
