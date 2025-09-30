{{
    config(
        materialized = 'ephemeral',
        tags = ['gscv_oti_s26_dcreceipts_grandmaster','gscv_oti_s26_150_arthur_dly_dc_rcpt_child'],
        alias = 'gscv_dly_dc_rcpt_lookup_rejects_appended_1'
    )
}}

WITH Js_Out_Rej AS (
SELECT
  'MD5' AS oti_dc_rcpt_rjct_id,
  row6.err_cd AS err_cd,
  row6.wrin AS item_number,
  row6.wsi AS wsi,
  row6.dc_fcil_id AS dc,
  row6.dc_terr_cd AS countrycode,
  row6.dc_trsf_fcil_id AS transdc,
  row6.dc_rcpt_dt AS daterc,
  row6.shpg_unt_case_recv_qt AS cases,
  row6.fca_case_cost_am AS fcacost,
  row6.fcacurr AS fcacurr,
  row6.fdc_case_cost_am AS freedc,
  row6.dccurr AS freedccurr,
  row6.po_ln_id AS poline,
  row6.po_id AS ponum,
  row6.dc_rcpt_xtrc_dt AS transdate,
  row6.raw_itm_lcl_fl AS localflag,
  row6.gbal_trad_itm_nu AS gtin,
  row6.dc_trsf_fcil_gln_id AS transdc_gln,
  row6.fcil_gln_id AS facility_gln,
  row6.dc_gln_id AS dc_gln,
  CASE
    WHEN row6.terr_cd_mapped IS NULL OR CAST(row6.terr_cd_mapped AS STRING) = '' THEN 0
    ELSE CAST(row6.terr_cd_mapped AS INT64)
  END AS terr_cd,
  CAST(row6.Timestamp AS TIMESTAMP) AS srce_file_recv_ts,
  CURRENT_TIMESTAMP() AS fst_occr_dt,
  CURRENT_TIMESTAMP() AS ltst_reoc_dt,
  1 AS cnt_of_occr_nu,
  CAST(NULL AS DATE) AS rsol_dt,
  CAST(NULL AS STRING) AS sevr_typ,
  CAST(NULL AS STRING) AS cmnt_tx,
  CAST(0 AS INT64) AS rec_serl_nu,
  row7.dw_file_na AS srce_file_nm,
  COALESCE(row6.dw_file_id, '0') AS dw_file_id,
  row6.SRCE_ERR_TYP_ID AS SRCE_ERR_TYP_ID,
  row6.DQ_RSPN_TYP_ID AS DQ_RSPN_TYP_ID
FROM
  {{ source('MacD_GSCV_Dev','JS_IN_GSCV_DLY_DC_RCPT_LOOKUP_REJECTS_tFileInputDelimited_11') }} AS row6
LEFT JOIN
  {{ source('MacD_GSCV_Dev','JS_IN_FILE_ATTRB_tFileInputDelimited_12') }} AS row7
ON
  row6.dw_file_id = row7.dw_file_id
),
Valid AS (
SELECT
  Js.oti_dc_rcpt_rjct_id,
  r1.schn_dq_rule_id AS err_cd,
  Js.item_number,
  Js.wsi,
  Js.dc,
  Js.countrycode,
  Js.transdc,
  Js.daterc,
  Js.cases,
  Js.fcacost,
  Js.fcacurr,
  Js.freedc,
  Js.freedccurr,
  Js.poline,
  Js.ponum,
  Js.transdate,
  Js.localflag,
  Js.gtin,
  Js.transdc_gln,
  Js.facility_gln,
  Js.dc_gln,
  Js.terr_cd,
  Js.srce_file_recv_ts,
  Js.fst_occr_dt,
  Js.ltst_reoc_dt,
  COALESCE(Js.cnt_of_occr_nu, 0) AS cnt_of_occr_nu,
  Js.rsol_dt,
  r1.xcpt_sevr_cd AS sevr_typ,
  r1.err_msg_cat_tx AS err_ds,
  Js.cmnt_tx,
  Js.rec_serl_nu,
  Js.srce_file_nm,
  Js.dw_file_id,
  Js.SRCE_ERR_TYP_ID,
  Js.DQ_RSPN_TYP_ID
FROM
  Js_Out_Rej AS Js
JOIN
  {{ source('data_audit','schn_oti_objt_dq_rule_assc') }} AS r1
ON
  Js.err_cd = r1.schn_dq_rule_id
),
tUniqRow_1 AS (
  SELECT *,
        ROW_NUMBER() OVER (PARTITION BY err_cd, item_number, wsi, dc, countrycode,
        transdc, daterc, cases, fcacost, fcacurr, freedc, freedccurr, poline, ponum,
        transdate, localflag, gtin, transdc_gln, facility_gln, dc_gln, terr_cd 
      ) AS rn 
      FROM Valid
)
SELECT * EXCEPT(rn) FROM tUniqRow_1 WHERE rn = 1