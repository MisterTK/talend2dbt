{{ config(
    materialized='table', 
    alias='gscv_dly_dc_rcpt_valid_reject',
    tags=['gscv_s77_dc_receipts_oti_recon_grandmaster','gscv_s77_100_dc_receipts_oti_recon_child']
) }}

SELECT
    main.wrin,
    main.dc AS dc_fcil_id, -- Note: Talend reject output has these as STRING
    main.countrycode AS dc_terr_cd, -- Note: Talend reject output has these as STRING
    main.transdate AS dc_rcpt_xtrc_dt, -- Note: Talend reject output has these as STRING
    main.ponum AS po_id,
    main.poline AS po_ln_id, -- Note: Talend reject output has these as STRING
    main.wsi, -- Note: Talend reject output has these as STRING
    CAST(NULL AS STRING) AS shpg_unt_nu, -- NULL if no match, type STRING as per Talend reject metadata
    CAST(NULL AS STRING) AS fca_lcl_curn_iso_nu, -- NULL if no match, type STRING
    CAST(NULL AS STRING) AS fdc_lcl_curn_iso_nu, -- NULL if no match, type STRING
    main.daterec AS dc_rcpt_dt, -- Note: Talend reject output has these as STRING
    main.transdc AS dc_trsf_fcil_id, -- Note: Talend reject output has these as STRING
    main.cases AS shpg_unt_case_recv_qt, -- Note: Talend reject output has these as STRING
    main.fcacost AS fca_case_cost_am, -- Note: Talend reject output has these as STRING
    main.dccost AS fdc_case_cost_am, -- Note: Talend reject output has these as STRING
    main.localflag AS raw_itm_lcl_fl, -- Note: Talend reject output has these as STRING
    main.fcacost AS gbal_fca_case_cost_am, -- Note: Talend reject output has these as STRING
    main.dccost AS gbal_fdc_case_cost_am, -- Note: Talend reject output has these as STRING
    '840' AS gbal_curn_iso_nu, -- Hardcoded string value as per Talend reject output
    main.dw_file_id,
    main.dc_gln AS dc_gln_id,
    main.transdc_gln AS dc_trsf_fcil_gln_id,
    main.facility_gln AS fcil_gln_id,
    CAST(NULL AS STRING) AS dc_pri_gln_id, -- Empty string in Talend maps to NULL
    CAST(NULL AS STRING) AS dc_pri_trsf_fcil_gln_id, -- Empty string in Talend maps to NULL
    CAST(NULL AS STRING) AS fcil_pri_gln_id, -- Empty string in Talend maps to NULL
    main.gtin AS gbal_trad_itm_nu,
    CAST(NULL AS STRING) AS euro_fca_case_cost_am, -- Not explicitly mapped in Talend reject, use NULL
    CAST(NULL AS STRING) AS euro_fdc_case_cost_am, -- Not explicitly mapped in Talend reject, use NULL
    CAST(NULL AS STRING) AS euro_curn_iso_nu, -- Not explicitly mapped in Talend reject, use NULL
    main.timestampp, -- Assumes 'timestamp' is already a TIMESTAMP/DATE type from upstream
    CAST(main.terr_cd AS INT64) AS terr_cd_mapped, -- Cast to INT64 as per Talend reject metadata
    'OTI_DCRECD_0135' AS err_cd, -- Hardcoded error code for rejected rows
    main.fcacurr,
    main.dccurr,
    main.srce_file_nm,
    main.oti_dc_rcpt_rjct_id,
    CAST(main.oti_dq_srce_err_typ_id AS INT64) AS oti_dq_srce_err_typ_id,
    CAST(main.oti_dq_rspn_typ_id AS INT64) AS oti_dq_rspn_typ_id
FROM
    {{ ref('gscv_dly_dc_rcpt_valid') }} AS main
LEFT JOIN
    {{ ref('rs_in_shpg_unt') }} AS shpg_unt
    ON COALESCE(NULLIF(REGEXP_REPLACE(main.wrin, '^0+', ''), ''), '0') = shpg_unt.XTRN_RAW_ITM_NU
LEFT JOIN
    {{ ref('gscv_curn_psv') }} AS curn1
    ON main.fcacurr = curn1.curn_iso3_abbr_cd
LEFT JOIN
    {{ ref('gscv_curn_psv') }} AS curn2
    ON main.dccurr = curn2.curn_iso3_abbr_cd
WHERE
    shpg_unt.SHPG_UNT_NU IS NULL 
    OR curn1.curn_iso_nu IS NULL 
    OR curn2.curn_iso_nu IS NULL 