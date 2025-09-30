{{ config(
    materialized='table',
    alias='dly_dc_rcpt_valid_3',
    tags=['gscv_s77_dc_receipts_oti_recon_grandmaster','gscv_s77_100_dc_receipts_oti_recon_child']
) }}

SELECT
    main.wrin,
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
    main.dw_file_id,
    main.dc_gln_id,
    main.dc_trsf_fcil_gln_id,
    main.fcil_gln_id,
    main.dc_pri_gln_id,
    main.dc_pri_trsf_fcil_gln_id,
    main.fcil_pri_gln_id,
    main.gbal_trad_itm_nu,
    CASE
        WHEN main.gbal_fca_case_cost_am IS NULL THEN NULL
        ELSE SAFE_DIVIDE(main.gbal_fca_case_cost_am, Dly_Curn_Xcng_Rate_3.dly_xcng_rate_nu)
    END AS euro_fca_case_cost_am,
    CASE
        WHEN main.gbal_fdc_case_cost_am IS NULL THEN NULL
        ELSE SAFE_DIVIDE(main.gbal_fdc_case_cost_am, Dly_Curn_Xcng_Rate_3.dly_xcng_rate_nu)
    END AS euro_fdc_case_cost_am,
    978 AS euro_curn_iso_nu,
    main.timestampp,
    main.terr_cd_mapped,
    main.fcacurr,
    main.dccurr,
    main.localflag,
    main.transdc_gln,
    main.terr_cd,
    main.srce_file_nm,
    main.oti_dc_rcpt_rjct_id,
    main.oti_dq_srce_err_typ_id,
    main.oti_dq_rspn_typ_id
FROM
    {{ ref('dly_dc_rcpt_valid_2') }} AS main
INNER JOIN
    {{ ref('dly_curn_xcng_rate_psv') }} AS Dly_Curn_Xcng_Rate_3
    ON main.dc_rcpt_dt = Dly_Curn_Xcng_Rate_3.cal_dt
    AND 978 = Dly_Curn_Xcng_Rate_3.from_curn_iso_nu