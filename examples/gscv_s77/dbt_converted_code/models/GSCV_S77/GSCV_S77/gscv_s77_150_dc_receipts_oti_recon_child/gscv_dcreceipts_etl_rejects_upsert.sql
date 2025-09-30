{{
    config(
        materialized='table', 
        alias='gscv_dcreceipts_etl_rejects_upsert',
        tags=['gscv_s77_dc_receipts_oti_recon_grandmaster','gscv_s77_150_dc_receipts_oti_recon_child']
    )
}}

SELECT
   oti_dc_rcpt_rjct_id,
    err_cd,
    item_number,
    wsi,
    dc,
    countrycode,
    transdc,
    CAST(daterec AS DATE) AS daterec, -- Consistent DATE cast for UNION ALL
    cases,
    fcacost,
    fcacurr,
    freedc,
    freedccurr,
    poline,
    ponum,
    CAST(transdate AS DATE) AS transdate,
    localflag,
    gtin,
    transdc_gln,   -- Re-ordered to match INSERT list
    facility_gln,
    dc_gln,        -- Re-ordered to match INSERT list
    terr_cd,
    CAST(srce_file_recv_ts AS TIMESTAMP) AS srce_file_recv_ts,
    fst_occr_dt,
    ltst_reoc_dt,
    cnt_of_occr_nu,
    -- **** CRITICAL CHANGES START HERE (to match the INSERT list order) ****
    CAST(NULL AS TIMESTAMP) AS rsol_dt, -- This is now at position 27, and correctly TIMESTAMP
                                        -- If rsol_dt comes from a source column and is STRING, use:
                                        -- CAST(your_source_rsol_dt_column AS TIMESTAMP) AS rsol_dt
    CAST(sevr_typ AS STRING) AS sevr_typ, -- Now at position 28
    err_ds,
    cmnt_tx, -- Re-ordered to match INSERT list (was near the end)
    CAST(0 AS INT64) AS rec_serl_nu, -- Now at position 31
    srce_file_nm,
    dw_file_id,
    oti_dq_srce_err_typ_id,
    oti_dq_rspn_typ_id
FROM
    {{ ref('gscv_dcreceipts_etl_rejects_upsert_1') }}

UNION ALL

SELECT
    oti_dc_rcpt_rjct_id,
    err_cd,
    item_number,
    wsi,
    dc,
    countrycode,
    transdc,
    CAST(daterec AS DATE) AS daterec, -- Consistent DATE cast for UNION ALL
    cases,
    fcacost,
    fcacurr,
    freedc,
    freedccurr,
    poline,
    ponum,
    CAST(transdate AS DATE) AS transdate,
    localflag,
    gtin,
    transdc_gln,   -- Re-ordered to match INSERT list
    facility_gln,
    dc_gln,        -- Re-ordered to match INSERT list
    terr_cd,
    CAST(srce_file_recv_ts AS TIMESTAMP) AS srce_file_recv_ts,
    fst_occr_dt,
    ltst_reoc_dt,
    cnt_of_occr_nu,
    -- **** CRITICAL CHANGES START HERE (to match the INSERT list order) ****
    CAST(NULL AS TIMESTAMP) AS rsol_dt, -- This is now at position 27, and correctly TIMESTAMP
                                        -- If rsol_dt comes from a source column and is STRING, use:
                                        -- CAST(your_source_rsol_dt_column AS TIMESTAMP) AS rsol_dt
    CAST(sevr_typ AS STRING) AS sevr_typ, -- Now at position 28
    err_ds,
    cmnt_tx, -- Re-ordered to match INSERT list (was near the end)
    CAST(0 AS INT64) AS rec_serl_nu, -- Now at position 31
    srce_file_nm,
    dw_file_id,
    oti_dq_srce_err_typ_id,
    oti_dq_rspn_typ_id
FROM
    {{ ref('gscv_dcreceipts_etl_rejects_upsert_2') }} 