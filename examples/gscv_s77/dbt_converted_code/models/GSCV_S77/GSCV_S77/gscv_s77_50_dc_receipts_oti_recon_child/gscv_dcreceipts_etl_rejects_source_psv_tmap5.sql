{{ config(
    materialized='table',
    alias='gscv_dcreceipts_etl_rejects_source_psv_tmap5',
    tags=['gscv_s77_dc_receipts_oti_recon_grandmaster','gscv_s77_50_dc_receipts_oti_recon_child']
) }}

SELECT
    wrin,
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
    timestampp, 
    srce_file_nm,
    dw_file_id,
    oti_dc_rcpt_rjct_id,
    oti_dq_srce_err_typ_id,
    oti_dq_rspn_typ_id
FROM
    {{ ref('gscv_dcreceipts_etl_rejects_extract_psv_tmap4') }} -- Reference the upstream model that provides the input data
WHERE
    ddr_updt_dw_audt_ts IS NULL
    OR
    CAST(oti_updt_dw_audt_ts AS TIMESTAMP) > CAST(ddr_updt_dw_audt_ts AS timestamp)