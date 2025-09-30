{{ config(
    materialized='table',
    alias='gscv_dcreceipts_fk_resolved_psv_tmap1',
    tags=['gscv_s77_dc_receipts_oti_recon_grandmaster','gscv_s77_50_dc_receipts_oti_recon_child'] 
) }}

{% set comment_text = var('resolution_comment', 'Default resolution comment') %}

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
    -- Columns explicitly set to NULL in Talend, cast to appropriate types for BigQuery
    CAST(NULL AS TIMESTAMP) AS srce_file_recv_ts,
    CAST(NULL AS TIMESTAMP) AS fst_occr_dt,
    CAST(NULL AS TIMESTAMP) AS ltst_reoc_dt,
    CAST(NULL AS INT64) AS cnt_of_occr_nu,
    -- Current timestamp for rsol_dt
    CURRENT_TIMESTAMP() AS rsol_dt,
    sevr_typ,
    '' as err_ds,
    -- Context variable for comment_tx
    '{{ comment_text }}' AS cmnt_tx,
    '' as rec_serl_nu,
    srce_file_nm,
    dw_file_id,
    oti_dq_srce_err_typ_id,
    oti_dq_rspn_typ_id
FROM
    {{ ref('gscv_dcreceipts_dq_extract_psv_tmap4') }} -- This references your upstream dbt model.
WHERE
    -- Talend filter: (oti_updt_dw_audt_ts < ddr_updt_dw_audt_ts) AND (sevr_typ = 'CRITICAL')
    CAST(oti_updt_dw_audt_ts AS TIMESTAMP) < CAST(ddr_updt_dw_audt_ts AS TIMESTAMP)
    AND sevr_typ = 'CRITICAL'