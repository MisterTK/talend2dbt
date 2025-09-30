{{ config(
    materialized='view',
    alias='hash_Raw_Item_Rejects',
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
    main.fca_case_cost_am,
    main.fdc_case_cost_am,
    CAST(main.localflag AS STRING) AS raw_itm_lcl_fl, -- Mapped from localflag and cast to STRING
    main.gbal_fca_case_cost_am,
    main.gbal_fdc_case_cost_am,
    main.gbal_curn_iso_nu,
    main.dw_file_id,
    main.dc_gln_id,
    main.transdc_gln AS dc_trsf_fcil_gln_id, -- Mapped from transdc_gln in reject flow
    main.fcil_gln_id,
    main.dc_pri_gln_id,
    main.dc_pri_trsf_fcil_gln_id,
    main.fcil_pri_gln_id,
    main.gbal_trad_itm_nu,
    main.euro_fca_case_cost_am,
    main.euro_fdc_case_cost_am,
    main.euro_curn_iso_nu,
    main.timestampp,
    main.terr_cd AS terr_cd_mapped, -- terr_cd from input for reject flow
    CAST(NULL AS STRING) AS err_cd, -- Explicitly NULL as per Talend
    main.fcacurr,
    main.dccurr,
    main.srce_file_nm,
    main.oti_dc_rcpt_rjct_id,
    main.oti_dq_srce_err_typ_id,
    main.oti_dq_rspn_typ_id
FROM
    {{ ref('dly_dc_rcpt_valid_3') }} AS main
LEFT JOIN
    {{ ref('rs_in_raw_itm') }} AS raw_item_lookup
    ON main.terr_cd_mapped = raw_item_lookup.terr_cd
    AND main.wrin = raw_item_lookup.xtrn_raw_itm_nu
WHERE
    raw_item_lookup.terr_cd IS NULL -- Filter for records where the join failed