{{ config(
    materialized='table',
    alias='dly_dc_rcpt_valid_2',
    tags=['gscv_s77_dc_receipts_oti_recon_grandmaster','gscv_s77_100_dc_receipts_oti_recon_child']
) }}

SELECT
    main.wrin,
    CAST(main.dc_fcil_id AS INT64) AS dc_fcil_id,
    CAST(main.dc_terr_cd AS INT64) AS dc_terr_cd,
    main.dc_rcpt_xtrc_dt,
    main.po_id,
    CAST(main.po_ln_id AS INT64) AS po_ln_id,
    CAST(main.wsi_nu AS INT64) AS wsi_nu,
    main.shpg_unt_nu, -- Direct pass-through
    main.fca_lcl_curn_iso_nu, -- Direct pass-through
    main.fdc_lcl_curn_iso_nu, -- Direct pass-through
    -- Date parsing for dc_rcpt_dt (from Dly_Dc_Rcpt_Valid_1.dc_rcpt_dt)
    -- Assuming dc_rcpt_dt is already a proper DATE type in Dly_Dc_Rcpt_Valid_1, no further parsing needed.
    -- If it's a string like "ddMMyyyy", use PARSE_DATE('%d%m%Y', main.dc_rcpt_dt)
    main.dc_rcpt_dt,
    CAST(main.dc_trsf_fcil_id AS INT64) AS dc_trsf_fcil_id,
    CAST(main.shpg_unt_case_recv_qt AS INT64) AS shpg_unt_case_recv_qt,
    main.fca_spnd_am AS fca_case_cost_am, -- Direct pass-through as per Talend, aliased as fca_case_cost_am
    main.fdc_spnd_am AS fdc_case_cost_am, -- Direct pass-through as per Talend, aliased as fdc_case_cost_am
    -- raw_itm_lcl_fl logic (from Dly_Dc_Rcpt_Valid_1.raw_itm_lcl_fl)
    main.raw_itm_lcl_fl,
    -- gbal_fca_case_cost_am calculation as per Talend (COALESCE handles ISNULL)
    COALESCE(main.fca_spnd_am * Dly_Curn_Xcng_Rate_1.dly_xcng_rate_nu, BIGNUMERIC '0.00') AS gbal_fca_case_cost_am,
    -- gbal_fdc_case_cost_am calculation as per Talend (COALESCE handles ISNULL)
    COALESCE(main.fdc_spnd_am * Dly_Curn_Xcng_Rate_2.dly_xcng_rate_nu, BIGNUMERIC '0.00') AS gbal_fdc_case_cost_am,
    main.gbal_curn_iso_nu,
    main.dw_file_id,
    main.dc_gln_id,
    main.dc_trsf_fcil_gln_id, -- Direct pass-through, assuming already STRING. If null in Talend, it's "" which is a string
    main.fcil_gln_id,
    CAST(NULL AS STRING) AS dc_pri_gln_id, -- Explicitly NULL as per Talend mapping
    CAST(NULL AS STRING) AS dc_pri_trsf_fcil_gln_id, -- Explicitly NULL
    CAST(NULL AS STRING) AS fcil_pri_gln_id, -- Explicitly NULL
    main.gbal_trad_itm_nu,
    main.timestampp, -- Use backticks if 'timestamp' is a reserved keyword
    -- terr_cd_mapped logic: if raw_itm_lcl_fl equals 1, then dc_terr_cd, else 0
    CASE
        WHEN main.raw_itm_lcl_fl = 1 THEN CAST(main.dc_terr_cd AS INT64)
        ELSE 0
    END AS terr_cd_mapped,
    main.fcacurr,
    main.dccurr,
    main.localflag,
    main.transdc_gln,
    CAST(main.terr_cd AS INT64) AS terr_cd,
    main.srce_file_nm,
    main.oti_dc_rcpt_rjct_id,
    CAST(main.oti_dq_srce_err_typ_id AS INT64) AS oti_dq_srce_err_typ_id,
    CAST(main.oti_dq_rspn_typ_id AS INT64) AS oti_dq_rspn_typ_id
FROM
    {{ ref('dly_dc_rcpt_valid_1') }} as main
inner join
    {{ ref('dly_curn_xcng_rate_psv') }} as dly_curn_xcng_rate_1 -- alias as per talend
    on main.dc_rcpt_dt = dly_curn_xcng_rate_1.cal_dt
    and main.fca_lcl_curn_iso_nu = dly_curn_xcng_rate_1.from_curn_iso_nu -- join on integer values
inner join
    {{ ref('dly_curn_xcng_rate_psv') }} as dly_curn_xcng_rate_2 -- alias as per talend
    on main.dc_rcpt_dt = dly_curn_xcng_rate_2.cal_dt
    and main.fdc_lcl_curn_iso_nu = dly_curn_xcng_rate_2.from_curn_iso_nu -- join on integer values