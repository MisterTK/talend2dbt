{{ config(
    materialized='table',
    alias='gscv_oti_recon_dly_dc_rcpt_final_psv',
    tags=['gscv_s77_dc_receipts_oti_recon_grandmaster','gscv_s77_100_dc_receipts_oti_recon_child']
) }}

SELECT
    dc_fcil_id,
    dc_terr_cd,
    dc_rcpt_xtrc_dt,
    po_id,
    po_ln_id,
    wsi_nu,
    shpg_unt_nu,
    fca_lcl_curn_iso_nu,
    fdc_lcl_curn_iso_nu,
    dc_rcpt_dt,
    dc_trsf_fcil_id,
    shpg_unt_case_recv_qt,
    cast(fca_case_cost_am as numeric) as fca_case_cost_am,
    cast(fdc_case_cost_am as numeric) as fdc_case_cost_am,
    raw_itm_lcl_fl,
    cast(gbal_fca_case_cost_am as numeric) as gbal_fca_case_cost_am ,
    cast(gbal_fdc_case_cost_am as numeric) as gbal_fdc_case_cost_am ,
    gbal_curn_iso_nu,
    dc_gln_id,
    dc_trsf_fcil_gln_id,
    fcil_gln_id,
    dc_pri_gln_id,
    dc_pri_trsf_fcil_gln_id,
    fcil_pri_gln_id,
    gbal_trad_itm_nu,
    cast(euro_fca_case_cost_am  as numeric) as euro_fca_case_cost_am,
    cast(euro_fdc_case_cost_am as numeric) as euro_fdc_case_cost_am ,
    euro_curn_iso_nu,
    TRIM(dw_file_id) AS dw_file_id
FROM
    {{ ref('gscv_oti_recon_dly_dc_rcpt') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY dc_fcil_id ORDER BY timestampp DESC) = 1