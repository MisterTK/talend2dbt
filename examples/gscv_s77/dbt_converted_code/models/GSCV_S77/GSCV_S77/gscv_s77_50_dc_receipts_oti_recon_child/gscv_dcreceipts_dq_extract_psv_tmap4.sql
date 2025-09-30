{{ config(
    materialized='table',
    alias='gscv_dcreceipts_dq_extract_psv_tmap4',
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

-- Step 1: Calculate the positions of the last underscore and last dot using REVERSE and STRPOS.
-- This is necessary because Redshift's STRPOS doesn't support a third 'start_position' argument.
transformed_data_step1 AS (
    SELECT
        *,
        -- Position of the last underscore, relative to the start of the string (1-based index)
        LENGTH(srce_file_nm) - STRPOS(REVERSE(srce_file_nm), '_') + 1 AS last_underscore_pos,
        -- Position of the last dot, relative to the start of the string (1-based index)
        LENGTH(srce_file_nm) - STRPOS(REVERSE(srce_file_nm), '.') + 1 AS last_dot_pos
    FROM
        source_data
),

-- Step 2: Extract the timestamp using the calculated positions.
transformed_data AS (
    SELECT
        *,
        SUBSTRING(
            srce_file_nm,
            last_underscore_pos + 1,                             -- Start extracting one character after the last underscore
            last_dot_pos - (last_underscore_pos + 1)             -- The length to extract
        ) AS file_timestamp_extracted_var
    FROM
        transformed_data_step1 -- Referencing the output of the first transformation step
)

-- Final SELECT statement: This is the equivalent of the 'out3' output stream (rejected rows based on original Talend logic).
SELECT
    ddr_updt_dw_audt_ts,
    oti_updt_dw_audt_ts,
    oti_dc_rcpt_rjct_id,
    err_cd,
    sevr_typ,
    item_number,
    wsi,
    dc,
    countrycode,
    transdc,
    daterec,
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
    terr_cd,
    file_timestamp_extracted_var AS timestampp, 
    srce_file_nm,
    dw_file_id,
    oti_dq_srce_err_typ_id,
    oti_dq_rspn_typ_id
FROM
    transformed_data
WHERE
    -- Inverse filter condition for 'out3': this means it selects rows *not* matching these error codes.
    err_cd NOT IN ('OTI_DCRECD_0135', 'OTI_DCRECD_0136', 'OTI_DCRECD_0137')