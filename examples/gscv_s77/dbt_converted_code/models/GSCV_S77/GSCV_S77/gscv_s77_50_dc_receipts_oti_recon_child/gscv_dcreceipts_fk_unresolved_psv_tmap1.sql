{{ config(
    materialized='table', 
    alias='gscv_dcreceipts_fk_unresolved_psv_tmap1',
    tags=['gscv_s77_dc_receipts_oti_recon_grandmaster','gscv_s77_50_dc_receipts_oti_recon_child']
) }}

SELECT
    oti_dc_rcpt_rjct_id,
    err_cd,
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
    timestampp,
    srce_file_nm,
    dw_file_id,
    oti_dq_srce_err_typ_id,
    oti_dq_rspn_typ_id
FROM
    {{ ref('gscv_dcreceipts_dq_extract_psv_tmap4') }} -- Same upstream model as the resolved data
WHERE
    -- Inverse of the Talend filter: NOT ((oti_updt_dw_audt_ts < ddr_updt_dw_audt_ts) AND (sevr_typ = 'CRITICAL'))
    -- Which simplifies to: (oti_updt_dw_audt_ts >= ddr_updt_dw_audt_ts) OR (sevr_typ != 'CRITICAL')
    NOT (CAST(oti_updt_dw_audt_ts AS TIMESTAMP) < CAST(ddr_updt_dw_audt_ts AS TIMESTAMP) AND sevr_typ = 'CRITICAL')