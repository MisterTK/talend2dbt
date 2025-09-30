{{
    config(
        materialized='ephemeral', 
        alias='etl_reject_insert_tmap_3',
        tags=['gscv_s77_dc_receipts_oti_recon_grandmaster','gscv_s77_150_dc_receipts_oti_recon_child']
    )
}}

SELECT
    row10.oti_dc_rcpt_rjct_id,
    row10.err_cd,
    row10.wrin AS item_number,
    row10.wsi,
    row10.dc_fcil_id AS dc,
    row10.dc_terr_cd AS countrycode,
    row10.dc_trsf_fcil_id AS transdc,
    row10.dc_rcpt_dt AS daterec,
    CAST(row10.shpg_unt_case_recv_qt AS STRING) AS cases,
    CAST(row10.fca_case_cost_am AS STRING) AS fcacost,
    row10.fcacurr,
    CAST(row10.fdc_case_cost_am AS STRING) AS freedc,
    row10.dccurr AS freedccurr,
    row10.po_id AS ponum,
    CAST(row10.po_ln_id AS STRING) AS poline,
    row10.dc_rcpt_xtrc_dt AS transdate,
    CAST(row10.raw_itm_lcl_fl AS STRING) AS localflag,
    row10.gbal_trad_itm_nu AS gtin,
    row10.dc_trsf_fcil_gln_id AS transdc_gln,
    row10.fcil_gln_id AS facility_gln,
    row10.dc_gln_id AS dc_gln,
    row10.terr_cd_mapped AS terr_cd,
    CAST(row10.Timestamp AS TIMESTAMP) AS srce_file_recv_ts,
    CURRENT_TIMESTAMP() AS fst_occr_dt,
    CURRENT_TIMESTAMP() AS ltst_reoc_dt,
    1 AS cnt_of_occr_nu,
    CAST(NULL AS TIMESTAMP) AS rsol_dt,
    CAST(NULL AS STRING) AS sevr_typ,
    CAST(NULL AS STRING) AS err_ds,
    '{{ var("cmnt", "Default Comment") }}' AS cmnt_tx,
    CAST(0 AS INT64) AS rec_serl_nu,
    row10.srce_file_nm,
    row10.dw_file_id,
    row10.oti_dq_srce_err_typ_id,
    row10.oti_dq_rspn_typ_id

FROM
     {{ ref('gscv_dcreceipts_etl_rejects_extract_psv_tmap4') }} AS row11
INNER JOIN
    {{ ref('gscv_dly_dc_rcpt_etl_rejects_psv') }} AS row10 ON
        row10.err_cd = row11.err_cd AND
        row10.oti_dc_rcpt_rjct_id = row11.oti_dc_rcpt_rjct_id