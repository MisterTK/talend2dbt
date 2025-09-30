{{ config(
    materialized='view', 
    alias='FK_OTI_RECON_rejects_GSCV_DCReceipts_FK_Source1_FINAL_psv',
    tags=['gscv_s77_dc_receipts_oti_recon_grandmaster','gscv_s77_50_dc_receipts_oti_recon_child']
) }}

SELECT
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
    oti_dc_rcpt_rjct_id,
    oti_dq_srce_err_typ_id,
    oti_dq_rspn_typ_id,
    recorded_date,
    severity,
    error_description,
    issue_code
FROM
    
    {{ source('FK_OTI_RECON', 'FK_OTI_RECON_rejects_GSCV_DCReceipts_FK_Source1_FINAL_psv') }}
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY
        oti_dc_rcpt_rjct_id,
        issue_code           
    ORDER BY
        recorded_date DESC,  
        timestampp DESC,      
        item_number,
        srce_file_nm,
        dw_file_id
) = 1