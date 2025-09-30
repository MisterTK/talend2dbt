{{ config(
    materialized='table', 
    alias='gscv_dly_dc_rcpt_valid',
    tags=['gscv_s77_dc_receipts_oti_recon_grandmaster','gscv_s77_100_dc_receipts_oti_recon_child']
) }}

SELECT
    stg_source.wrin,
    stg_source.wsi,
    stg_source.dc,
    stg_source.countrycode,
    stg_source.transdc,
    stg_source.daterec, 
    stg_source.cases,
    stg_source.fcacost,
    stg_source.fcacurr,
    stg_source.dccost,
    stg_source.dccurr,
    stg_source.ponum,
    stg_source.poline,
    stg_source.transdate, 
    stg_source.localflag,
    stg_source.gtin,
    stg_source.dc_gln,
    stg_source.transdc_gln,
    stg_source.facility_gln,
    CAST(stg_source.terr_cd AS INT64) AS terr_cd, 
    stg_source.dw_file_id,
    stg_source.srce_file_nm,
    -- Talend timestamp parsing logic translated to BigQuery's PARSE_TIMESTAMP
    CASE
        WHEN LENGTH(stg_source.timestampp) > 14
            THEN PARSE_TIMESTAMP('%Y%m%d%H%M%S%f', stg_source.timestampp) 
        ELSE PARSE_TIMESTAMP('%Y%m%d%H%M%S', stg_source.timestampp)
    END AS timestampp, 
    stg_source.oti_dc_rcpt_rjct_id,
    CAST(stg_source.oti_dq_srce_err_typ_id AS INT64) AS oti_dq_srce_err_typ_id, -- Cast to INT64
    CAST(stg_source.oti_dq_rspn_typ_id AS INT64) AS oti_dq_rspn_typ_id -- Cast to INT64
FROM
    {{ ref('gscv_dcreceipts_fk_source_valid_final_psv') }} AS stg_source