{{ config(
    materialized='table', 
    alias='gscv_dcreceipts_fk_source_valid_final_psv',
    tags=['gscv_s77_dc_receipts_oti_recon_grandmaster','gscv_s77_50_dc_receipts_oti_recon_child']
) }}

SELECT
    tmap_source.wrin, 
    tmap_source.wsi,
    tmap_source.dc,
    tmap_source.countrycode,
    tmap_source.transdc,
    tmap_source.daterec, 
    tmap_source.cases,
    tmap_source.fcacost,
    tmap_source.fcacurr,
    tmap_source.dccost,
    tmap_source.dccurr,
    tmap_source.ponum,
    tmap_source.poline,
    tmap_source.transdate, 
    tmap_source.localflag,
    tmap_source.gtin,
    tmap_source.dc_gln,
    tmap_source.transdc_gln,
    tmap_source.facility_gln,
    CAST(tmap_source.terr_cd AS INT64) AS terr_cd, 
    tmap_source.timestampp, 
    tmap_source.srce_file_nm,
    tmap_source.dw_file_id,
    tmap_source.oti_dc_rcpt_rjct_id,
    CAST(tmap_source.oti_dq_srce_err_typ_id AS INT64) AS oti_dq_srce_err_typ_id, 
    CAST(tmap_source.oti_dq_rspn_typ_id AS INT64) AS oti_dq_rspn_typ_id 
FROM
    {{ source('FK_OTI_RECON', 'GSCV_DCReceipts_FK_Source_valid_psv') }} AS tmap_source 
INNER JOIN
    {{ ref('gscv_dcreceipts_dq_extract_psv_tmap4_only_critical') }} AS critical_lookup 
ON
    tmap_source.oti_dc_rcpt_rjct_id = critical_lookup.oti_dc_rcpt_rjct_id

