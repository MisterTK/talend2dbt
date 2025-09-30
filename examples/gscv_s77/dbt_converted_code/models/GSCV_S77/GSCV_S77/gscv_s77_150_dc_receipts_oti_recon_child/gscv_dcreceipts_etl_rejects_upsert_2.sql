{{
    config(
        materialized='ephemeral',
        alias='gscv_dcreceipts_etl_rejects_upsert_2',
       tags=['gscv_s77_dc_receipts_oti_recon_grandmaster','gscv_s77_150_dc_receipts_oti_recon_child']
    )
}}

WITH row1 AS (
    
    SELECT
        schn_dq_rule_id,
        err_msg_cat_tx,
        xcpt_sevr_cd
    FROM
        -- Using a dbt variable for the schema name, as it was 'context.Redshift_gdap_data_audit_schema' in Talend
        {{ source('Redshift_gdap_data_audit','schn_oti_objt_dq_rule_assc') }}
    WHERE
        schn_oti_objt_na = 'DC RECEIPTS DAILY ETL REJECTS'
)
-- models/GSCV_S77_DC_Receipts_OTI_Recon_GrandMaster/GSCV_S77_150_DC_Receipts_OTI_Recon_Child/GSCV_DCReceipts_ETL_Rejects_UPSERT.sql

SELECT
    oti_dc_rcpt_rjct_id,
    err_cd,
    item_number,
    wsi,
    dc,
    countrycode,
    transdc,
    CAST(daterec AS DATE) AS daterec, -- Ensure consistent DATE cast
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
    dc_gln,
    transdc_gln,
    facility_gln,
    terr_cd,
    CAST(srce_file_recv_ts AS TIMESTAMP) AS srce_file_recv_ts,
    fst_occr_dt,
    ltst_reoc_dt,
    cnt_of_occr_nu,
    -- THIS IS THE CRITICAL CHANGE FOR COLUMN 27 (rsol_dt):
    CAST(NULL AS TIMESTAMP) AS rsol_dt, -- <<<< MAKE SURE THIS IS PRESENT AND CORRECTLY CASTING TO TIMESTAMP
    -- If rsol_dt comes from an actual source column and is currently a STRING, it should be:
    -- CAST(your_source_rsol_dt_column AS TIMESTAMP) AS rsol_dt,
    cast(sevr_typ as string) as sevr_typ,
    err_ds,
    CAST(0 AS INT64) AS rec_serl_nu,
    srce_file_nm,
    dw_file_id,
    oti_dq_srce_err_typ_id,
    oti_dq_rspn_typ_id,
    cmnt_tx -- This was at the end, I'm assuming the target expects it here.
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
    CAST(daterec AS DATE) AS daterec, -- Ensure consistent DATE cast
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
    dc_gln,
    transdc_gln,
    facility_gln,
    terr_cd,
    CAST(srce_file_recv_ts AS TIMESTAMP) AS srce_file_recv_ts,
    fst_occr_dt,
    ltst_reoc_dt,
    cnt_of_occr_nu,
    CAST(NULL AS TIMESTAMP) AS rsol_dt, 
    cast(sevr_typ as string) as sevr_typ,
    err_ds,
    CAST(0 AS INT64) AS rec_serl_nu,
    srce_file_nm,
    dw_file_id,
    oti_dq_srce_err_typ_id,
    oti_dq_rspn_typ_id,
    cmnt_tx 
FROM
    {{ ref('etl_reject_insert_tmap_3') }} AS e 
INNER JOIN
    row1 AS row1
    ON e.err_cd = row1.schn_dq_rule_id
