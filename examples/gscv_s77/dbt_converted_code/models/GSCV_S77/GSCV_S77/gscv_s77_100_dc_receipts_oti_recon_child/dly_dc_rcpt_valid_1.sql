{{ config(
    materialized='table', 
    alias='dly_dc_rcpt_valid_1',
    tags=['gscv_s77_dc_receipts_oti_recon_grandmaster','gscv_s77_100_dc_receipts_oti_recon_child']
) }}

SELECT
    main.wrin,
    CAST(main.dc AS INT64) AS dc_fcil_id,
    CAST(main.countrycode AS INT64) AS dc_terr_cd,
    CASE
        WHEN LENGTH(main.transdate) = 7
            THEN PARSE_DATE('%d%m%Y', '0' || main.transdate)
        ELSE PARSE_DATE('%d%m%Y', main.transdate)
    END AS dc_rcpt_xtrc_dt,
    main.ponum AS po_id,
    CAST(main.poline AS INT64) AS po_ln_id,
    CAST(main.wsi AS INT64) AS wsi_nu,
    shpg_unt.SHPG_UNT_NU AS shpg_unt_nu, -- From Shpg_Unt lookup
    curn1.curn_iso_nu AS fca_lcl_curn_iso_nu, -- From Curn_1 lookup
    curn2.curn_iso_nu AS fdc_lcl_curn_iso_nu, -- From Curn_2 lookup
    -- Conditional date parsing for dc_rcpt_dt (daterec)
    CASE
        WHEN LENGTH(main.daterec) = 7
            THEN PARSE_DATE('%d%m%Y', '0' || main.daterec)
        ELSE PARSE_DATE('%d%m%Y', main.daterec)
    END AS dc_rcpt_dt,
    CAST(main.transdc AS INT64) AS dc_trsf_fcil_id,
    CAST(main.cases AS INT64) AS shpg_unt_case_recv_qt,
    -- BigDecimal conversion for fca_spnd_am (fcacost)
    CASE
        WHEN main.fcacost IS NULL OR main.fcacost = '' THEN NULL
        WHEN CONTAINS_SUBSTR(main.fcacost, ',') THEN CAST(REPLACE(main.fcacost, ',', '.') AS BIGNUMERIC)
        ELSE CAST(main.fcacost AS BIGNUMERIC)
    END AS fca_spnd_am,
    -- BigDecimal conversion for fdc_spnd_am (dccost)
    CASE
        WHEN main.dccost IS NULL OR main.dccost = '' THEN NULL
        WHEN CONTAINS_SUBSTR(main.dccost, ',') THEN CAST(REPLACE(main.dccost, ',', '.') AS BIGNUMERIC)
        ELSE CAST(main.dccost AS BIGNUMERIC)
    END AS fdc_spnd_am,
    -- raw_itm_lcl_fl logic (from localflag)
    CASE
        WHEN main.localflag IS NULL OR main.localflag = '' THEN 0
        WHEN UPPER(SUBSTR(main.localflag, 1, 1)) = 'N' THEN 0
        WHEN UPPER(SUBSTR(main.localflag, 1, 1)) = 'Y' THEN 1
        ELSE 0
    END AS raw_itm_lcl_fl,
    840 AS gbal_curn_iso_nu, -- Hardcoded integer value
    main.dw_file_id,
    main.timestampp, -- Assumes 'timestamp' is already a TIMESTAMP/DATE type from upstream model
    main.dc_gln AS dc_gln_id,
    CAST(NULL AS STRING) AS dc_trsf_fcil_gln_id, -- Empty string in Talend maps to NULL for BigQuery type safety
    main.facility_gln AS fcil_gln_id,
    main.gtin AS gbal_trad_itm_nu,
    main.fcacurr,
    main.dccurr,
    main.localflag, -- Original localflag column is retained as per Talend output schema
    main.transdc_gln,
    CAST(main.terr_cd AS INT64) AS terr_cd, -- terr_cd from input (different from dc_terr_cd derived from countrycode)
    main.srce_file_nm,
    main.oti_dc_rcpt_rjct_id,
    CAST(main.oti_dq_srce_err_typ_id AS INT64) AS oti_dq_srce_err_typ_id,
    CAST(main.oti_dq_rspn_typ_id AS INT64) AS oti_dq_rspn_typ_id
FROM
    {{ ref('gscv_dly_dc_rcpt_valid') }} AS main -- Corresponds to Talend's 'Dly_Dc_Rcpt_Valid' input
INNER JOIN
    {{ ref('rs_in_shpg_unt') }} AS shpg_unt -- Corresponds to Talend's 'Shpg_Unt' lookup
    -- Talend's 'replaceFirst("^0+(?!$)","")' for wrin
    ON COALESCE(NULLIF(REGEXP_REPLACE(main.wrin, '^0+', ''), ''), '0') = shpg_unt.XTRN_RAW_ITM_NU
INNER JOIN
    {{ ref('gscv_curn_psv') }} AS curn1 -- Corresponds to Talend's 'Curn_1' lookup
    ON main.fcacurr = curn1.curn_iso3_abbr_cd
INNER JOIN
    {{ ref('gscv_curn_psv') }} AS curn2 -- Corresponds to Talend's 'Curn_2' lookup
    ON main.dccurr = curn2.curn_iso3_abbr_cd