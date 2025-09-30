{{ config(
    materialized='table', 
    alias='gscv_dcreceipts_dq_extract_psv_tmap4_only_critical',
    tags=['gscv_s77_dc_receipts_oti_recon_grandmaster','gscv_s77_50_dc_receipts_oti_recon_child']
) }}

WITH source_tmap4_data AS (
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
        oti_dq_srce_err_typ_id,
        oti_dq_rspn_typ_id,
        timestampp 
    FROM
        {{ ref('gscv_dcreceipts_dq_extract_psv_tmap4') }} -- Referencing the output of the previous model
),
filtered_data AS (
    SELECT
        *
    FROM
        source_tmap4_data
    WHERE
        UPPER(sevr_typ) = 'CRITICAL'
),
unique_data AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY oti_dc_rcpt_rjct_id ORDER BY ddr_updt_dw_audt_ts DESC) AS rn
    FROM
        filtered_data
    QUALIFY ROW_NUMBER() OVER (PARTITION BY oti_dc_rcpt_rjct_id ORDER BY ddr_updt_dw_audt_ts DESC) = 1
)
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
    oti_dq_srce_err_typ_id,
    oti_dq_rspn_typ_id,
    timestampp 
FROM
    unique_data
ORDER BY
    oti_dc_rcpt_rjct_id DESC 