{{ config(
    alias='dly_dc_rcpt',
    schema=var('Redshift_gdap_Schema'),
    materialized='incremental',
    unique_key=[
        'dc_terr_cd',
        'dc_rcpt_dt',
        'po_id',
        'po_ln_id',
        'dc_fcil_id',
        'wsi_nu',
        'shpg_unt_nu'
    ],
    incremental_strategy='merge',
    tags=['gscv_s77_dc_receipts_oti_recon_grandmaster','gscv_s77_100_dc_receipts_oti_recon_child']
) }}

--{% set wait_for_union = ref('gscv_oti_recon_dly_dc_rcpt_final_psv') %}

SELECT
    dc_terr_cd,
    dc_rcpt_dt,
    po_id,
    po_ln_id,
    dc_fcil_id,
    wsi_nu,
    shpg_unt_nu,
    fca_lcl_curn_iso_nu,
    fdc_lcl_curn_iso_nu,
    dc_rcpt_xtrc_dt,
    dc_trsf_fcil_id,
    shpg_unt_case_recv_qt,
    fca_case_cost_am,
    fdc_case_cost_am,
    raw_itm_lcl_fl,
    gbal_fca_case_cost_am,
    gbal_fdc_case_cost_am,
    gbal_curn_iso_nu,
    CAST(CURRENT_TIMESTAMP() AS DATETIME) AS LOAD_DW_AUDT_TS,
    CAST(CURRENT_TIMESTAMP() AS DATETIME) AS UPDT_DW_AUDT_TS,
    dc_gln_id,
    dc_trsf_fcil_gln_id,
    fcil_gln_id,
    gbal_trad_itm_nu,
    dc_pri_gln_id,
    dc_pri_trsf_fcil_gln_id,
    fcil_pri_gln_id,
    euro_fca_case_cost_am,
    euro_fdc_case_cost_am,
    euro_curn_iso_nu,
    dw_file_id
FROM
    {{ source('Redshift_gdap_Stage', 'stg_dly_dc_rcpt') }} -- Assuming 'stg_dly_dc_rcpt' is the dbt model representing your staging table