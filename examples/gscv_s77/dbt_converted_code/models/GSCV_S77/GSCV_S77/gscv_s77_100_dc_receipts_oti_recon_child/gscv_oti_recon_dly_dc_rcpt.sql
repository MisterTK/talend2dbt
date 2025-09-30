{{ config(
    materialized='table',
    alias='gscv_oti_recon_dly_dc_rcpt',
    tags=['gscv_s77_dc_receipts_oti_recon_grandmaster','gscv_s77_100_dc_receipts_oti_recon_child']
) }}

{% set wait_for_union = ref('rs_in_raw_itm') %}

SELECT
    main.dc_fcil_id,
    main.dc_terr_cd,
    main.dc_rcpt_xtrc_dt,
    main.po_id,
    main.po_ln_id,
    main.wsi_nu,
    main.shpg_unt_nu,
    main.fca_lcl_curn_iso_nu,
    main.fdc_lcl_curn_iso_nu,
    main.dc_rcpt_dt,
    main.dc_trsf_fcil_id,
    main.shpg_unt_case_recv_qt,
    main.fca_case_cost_am,
    main.fdc_case_cost_am,
    main.raw_itm_lcl_fl,
    main.gbal_fca_case_cost_am,
    main.gbal_fdc_case_cost_am,
    main.gbal_curn_iso_nu,
    main.dc_gln_id,
    main.dc_trsf_fcil_gln_id,
    main.fcil_gln_id,
    main.dc_pri_gln_id,
    main.dc_pri_trsf_fcil_gln_id,
    main.fcil_pri_gln_id,
    main.gbal_trad_itm_nu,
    main.euro_fca_case_cost_am,
    main.euro_fdc_case_cost_am,
    main.euro_curn_iso_nu,
    TRIM(main.dw_file_id) AS dw_file_id,
    CURRENT_TIMESTAMP() as timestampp    
FROM
    {{ ref('dly_dc_rcpt_valid_3') }} AS main
INNER JOIN
    {{ ref('rs_in_raw_itm') }} AS raw_item_lookup
    ON main.terr_cd_mapped = raw_item_lookup.terr_cd
    AND main.wrin = raw_item_lookup.xtrn_raw_itm_nu