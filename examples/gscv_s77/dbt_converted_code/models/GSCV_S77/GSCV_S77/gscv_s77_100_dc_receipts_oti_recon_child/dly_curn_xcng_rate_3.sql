{{ config(
    materialized='table',
    alias='dly_curn_xcng_rate_3',
    tags=['gscv_s77_dc_receipts_oti_recon_grandmaster','gscv_s77_100_dc_receipts_oti_recon_child']
) }}

SELECT
    main.wrin,
    main.dc_fcil_id,
    main.dc_terr_cd,
    FORMAT_DATE('%d%m%Y', main.dc_rcpt_xtrc_dt) AS dc_rcpt_xtrc_dt, 
    main.po_id,
    main.po_ln_id,
    main.wsi_nu,
    main.shpg_unt_nu,
    main.fca_lcl_curn_iso_nu,
    main.fdc_lcl_curn_iso_nu,
    FORMAT_DATE('%d%m%Y', main.dc_rcpt_dt) AS dc_rcpt_dt, 
    main.dc_trsf_fcil_id,
    main.shpg_unt_case_recv_qt,
    main.fca_case_cost_am,
    main.fdc_case_cost_am,
    CAST(main.raw_itm_lcl_fl AS STRING) AS raw_itm_lcl_fl, 
    main.gbal_fca_case_cost_am,
    main.gbal_fdc_case_cost_am,
    main.gbal_curn_iso_nu,
    main.dw_file_id,
    main.dc_gln_id,
    main.transdc_gln AS dc_trsf_fcil_gln_id, 
    main.fcil_gln_id,
    main.dc_pri_gln_id,
    main.dc_pri_trsf_fcil_gln_id,
    main.fcil_pri_gln_id,
    main.gbal_trad_itm_nu,
    CAST(NULL AS BIGNUMERIC) AS euro_fca_case_cost_am, 
    CAST(NULL AS BIGNUMERIC) AS euro_fdc_case_cost_am, 
    CAST(NULL AS INT64) AS euro_curn_iso_nu, -- Null for rejected records
    main.timestampp,
    main.terr_cd AS terr_cd_mapped, -- terr_cd from input for reject flow
    'OTI_DCRECD_0136' AS err_cd, -- Hardcoded error code
    main.fcacurr,
    main.dccurr,
    main.srce_file_nm,
    main.oti_dc_rcpt_rjct_id,
    main.oti_dq_srce_err_typ_id,
    main.oti_dq_rspn_typ_id
FROM
    {{ ref('dly_dc_rcpt_valid_2') }} AS main
LEFT JOIN
    {{ ref('dly_curn_xcng_rate_psv') }} AS Dly_Curn_Xcng_Rate_3
    ON main.dc_rcpt_dt = Dly_Curn_Xcng_Rate_3.cal_dt
    AND 978 = Dly_Curn_Xcng_Rate_3.from_curn_iso_nu
WHERE
    Dly_Curn_Xcng_Rate_3.dly_xcng_rate_nu IS NULL -- Filter for records where the join failed