{{ config(
    materialized='table',
    alias='dly_curn_xcng_reject',
    tags=['gscv_s77_dc_receipts_oti_recon_grandmaster','gscv_s77_100_dc_receipts_oti_recon_child']
) }}

SELECT
    main.wrin,
    main.dc_fcil_id,
    main.dc_terr_cd,
    FORMAT_DATE('%d%m%Y', main.dc_rcpt_xtrc_dt) AS dc_rcpt_xtrc_dt, -- Format back to string
    main.po_id,
    main.po_ln_id,
    main.wsi_nu,
    main.shpg_unt_nu,
    main.fca_lcl_curn_iso_nu,
    main.fdc_lcl_curn_iso_nu,
    FORMAT_DATE('%d%m%Y', main.dc_rcpt_dt) AS dc_rcpt_dt, -- Format back to string
    main.dc_trsf_fcil_id,
    main.shpg_unt_case_recv_qt,
    main.fca_spnd_am,
    main.fdc_spnd_am,
    main.raw_itm_lcl_fl, -- Talend maps this as String in reject flow
    main.fca_spnd_am AS gbal_fca_case_cost_am, -- Direct pass-through, no exchange rate for rejects
    main.fdc_spnd_am AS gbal_fdc_case_cost_am, -- Direct pass-through, no exchange rate for rejects
    main.gbal_curn_iso_nu,
    main.dw_file_id,
    main.dc_gln_id,
    main.transdc_gln AS dc_trsf_fcil_gln_id, -- Maps from transdc_gln in reject
    main.fcil_gln_id,
    CAST(NULL AS STRING) AS dc_pri_gln_id,
    CAST(NULL AS STRING) AS dc_pri_trsf_fcil_gln_id,
    CAST(NULL AS STRING) AS fcil_pri_gln_id,
    main.gbal_trad_itm_nu,
    CAST(NULL AS STRING) AS euro_fca_case_cost_am, -- Not mapped from input in Talend reject
    CAST(NULL AS STRING) AS euro_fdc_case_cost_am, -- Not mapped from input in Talend reject
    CAST(NULL AS STRING) AS euro_curn_iso_nu, -- Not mapped from input in Talend reject
    main.timestampp, -- Use backticks
    main.terr_cd AS terr_cd_mapped, -- terr_cd from input for reject flow
    'OTI_DCRECD_0136' AS err_cd, -- Hardcoded error code
    main.fcacurr,
    main.dccurr,
    main.srce_file_nm,
    main.oti_dc_rcpt_rjct_id,
    main.oti_dq_srce_err_typ_id
FROM
    {{ ref('dly_dc_rcpt_valid_1') }} AS main
LEFT JOIN
    {{ ref('dly_curn_xcng_rate_psv') }} AS Dly_Curn_Xcng_Rate_1
    ON main.dc_rcpt_dt = Dly_Curn_Xcng_Rate_1.cal_dt
    AND main.fca_lcl_curn_iso_nu = Dly_Curn_Xcng_Rate_1.from_curn_iso_nu
LEFT JOIN
    {{ ref('dly_curn_xcng_rate_psv') }} AS Dly_Curn_Xcng_Rate_2
    ON main.dc_rcpt_dt = Dly_Curn_Xcng_Rate_2.cal_dt
    AND main.fdc_lcl_curn_iso_nu = Dly_Curn_Xcng_Rate_2.from_curn_iso_nu
WHERE
    Dly_Curn_Xcng_Rate_1.dly_xcng_rate_nu IS NULL -- Where the first lookup failed
    OR Dly_Curn_Xcng_Rate_2.dly_xcng_rate_nu IS NULL -- Where the second lookup failed