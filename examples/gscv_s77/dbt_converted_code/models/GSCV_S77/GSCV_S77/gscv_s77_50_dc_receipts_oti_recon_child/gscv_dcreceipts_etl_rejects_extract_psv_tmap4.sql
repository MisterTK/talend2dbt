{{ config(
    materialized='view' ,
    alias='gscv_dcreceipts_etl_rejects_extract_psv_tmap4',
    tags=['gscv_s77_dc_receipts_oti_recon_grandmaster','gscv_s77_50_dc_receipts_oti_recon_child']
) }}

WITH source_data AS (
    SELECT
        ddr_updt_dw_audt_ts,
        oti_updt_dw_audt_ts,
        oti_dc_rcpt_rjct_id,
        err_cd,
        sevr_typ,
        terr_cd,
        srce_file_nm,
        dw_file_id,
        item_number,
        wsi,
        dc,
        countrycode,
        transdc,
        daterc AS daterec, 
        cases,
        fcacost,
        fcacurr,
        freedc, 
        freedccurr, 
        ponum,
        poline,
        transdate,
        localflag,
        gtin,
        dc_gln,
        transdc_gln,
        facility_gln,
        oti_dq_srce_err_typ_id,
        oti_dq_rspn_typ_id
    FROM
        {{ ref('rs_in_oti') }} 
),

transformed_data_step1 AS (
    SELECT
        *,
      
        LENGTH(srce_file_nm) - STRPOS(REVERSE(srce_file_nm), '_') + 1 AS last_underscore_pos,
        -- Position of the last dot, relative to the start of the string (1-based index)
        LENGTH(srce_file_nm) - STRPOS(REVERSE(srce_file_nm), '.') + 1 AS last_dot_pos
    FROM
        source_data
),


transformed_data AS (
    SELECT
        *,
        SUBSTRING(
            srce_file_nm,
            last_underscore_pos + 1,                            -- Start after the last underscore
            last_dot_pos - (last_underscore_pos + 1)            -- Length to extract
        ) AS timestamp_extract,
        -- Map freedc to dccost and freedccurr to dccurr based on original tMap logic
        freedc AS dccost,
        freedccurr AS dccurr,
        -- Rename item_number to wrin based on original tMap logic
        item_number AS wrin
    FROM
        transformed_data_step1 -- Use the results from the first transformation step
)

-- Final SELECT statement: This is the equivalent of the 'out2' output stream from your original logic
SELECT
    ddr_updt_dw_audt_ts,
    oti_updt_dw_audt_ts,
    oti_dc_rcpt_rjct_id,
    err_cd,
    sevr_typ,
    wrin, -- Mapped from item_number
    wsi,
    dc,
    countrycode,
    transdc,
    daterec,
    cases,
    fcacost,
    fcacurr,
    dccost, 
    dccurr, 
    ponum,
    poline,
    transdate,
    localflag,
    gtin,
    dc_gln,
    transdc_gln,
    facility_gln,
    terr_cd,
    timestamp_extract AS timestampp, 
    srce_file_nm,
    dw_file_id,
    oti_dq_srce_err_typ_id,
    oti_dq_rspn_typ_id
FROM
    transformed_data
WHERE
    err_cd IN ('OTI_DCRECD_0135', 'OTI_DCRECD_0136', 'OTI_DCRECD_0137')