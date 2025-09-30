{{
    config(
        materialized='ephemeral', 
        alias='gscv_dcreceipts_etl_rejects_upsert_1',
        tags=['gscv_s77_dc_receipts_oti_recon_grandmaster','gscv_s77_150_dc_receipts_oti_recon_child']
)}}
 
SELECT
    row8.oti_dc_rcpt_rjct_id,
    row8.err_cd,
    (row8.wrin) AS item_number,
    row8.wsi,
    row8.dc,
    row8.countrycode,
    row8.transdc,
    CAST(row8.daterec AS DATE) AS daterec, -- Assuming target daterc is DATE, and daterec in row8 is convertible to DATE
    row8.cases,
    row8.fcacost,
    row8.fcacurr,
    row8.dccost AS freedc,
    row8.dccurr AS freedccurr,
    row8.poline,
    row8.ponum,
    CAST(row8.transdate AS DATE) AS transdate,
    row8.localflag,
    row8.gtin,
    row8.transdc_gln,   -- Corrected position: This is now 19th (from INSERT list)
    row8.facility_gln,  -- Corrected position: This is now 20th
    row8.dc_gln,        -- Corrected position: This is now 21st
    row8.terr_cd,
    CAST(row8.timestampp AS TIMESTAMP) AS srce_file_recv_ts, -- Assuming row8.timestamp is convertible to TIMESTAMP
                                                            -- If it's a reserved word, you might need to backtick it: `row8.timestamp`
    CURRENT_TIMESTAMP() AS fst_occr_dt,
    CURRENT_TIMESTAMP() AS ltst_reoc_dt,
    1 AS cnt_of_occr_nu,
    -- **** Columns reordered to match INSERT list exactly from here ****
    CAST(NULL AS TIMESTAMP) AS rsol_dt, -- Corrected position: This is now 27th, as required by INSERT
    CAST(NULL AS STRING) AS sevr_typ,   -- Corrected position: This is now 28th
    CAST(NULL AS STRING) AS err_ds,     -- Corrected position: This is now 29th
    CAST(NULL AS STRING) AS cmnt_tx,    -- Corrected position: This is now 30th
    CAST(0 AS INT64) AS rec_serl_nu,    -- Corrected position: This is now 31st
    row8.srce_file_nm,                  -- Corrected position: This is now 32nd
    row8.dw_file_id,                    -- Corrected position: This is now 33rd
    row8.oti_dq_srce_err_typ_id,        -- Corrected position: This is now 34th
    row8.oti_dq_rspn_typ_id 
FROM
    {{ ref('gscv_dcreceipts_etl_rejects_extract_psv_tmap4') }} AS row8
INNER JOIN
    {{ ref('gscv_dly_dc_rcpt_etl_rejects_psv') }} AS row9 ON
        row8.err_cd = row9.err_cd AND
        row8.oti_dc_rcpt_rjct_id = row9.oti_dc_rcpt_rjct_id
