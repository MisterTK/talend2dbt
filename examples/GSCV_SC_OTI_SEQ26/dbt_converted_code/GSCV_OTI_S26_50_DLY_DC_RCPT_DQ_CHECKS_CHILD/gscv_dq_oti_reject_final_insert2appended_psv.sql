{{
    config(
        materialized = 'incremental',
        tags = ['gscv_oti_s26_dcreceipts_grandmaster','gscv_oti_s26_50_dly_dc_rcpt_dq_checks_child'],
        alias = 'gscv_dq_oti_reject_final_insert2appended_psv'
    )
}}

WITH Local_Items AS (
SELECT
  wrin,
  wsi,
  dc,
  countrycode,
  transdc,
  daterec,
  cases,
  fcacost,
  fcacurr,
  dccost,
  dccurr,
  ponum,
  poline,
  transdate,
  localflag,
  GTIN,
  DC_GLN,
  TransDC_GLN,
  DC_GLN_ID,
  SRCE_ERR_TYP_ID,
  Terr_Cd,
  Timestamp,
  Dw_File_Id,
  'OTI_DCRECD_0138' AS ERR_CD
FROM {{ source('MacD_GSCV_Dev','JS_IN_GSCV_DCReceipts_ILOS_csv_tFileInputDelimited_5') }}
WHERE UPPER(localflag) LIKE '%Y%'
),
Local_Item AS (
SELECT
  wrin,
  wsi,
  dc,
  countrycode,
  transdc,
  daterec,
  cases,
  fcacost,
  fcacurr,
  dccost,
  dccurr,
  ponum,
  poline,
  transdate,
  localflag,
  GTIN,
  DC_GLN,
  TransDC_GLN,
  DC_GLN_ID,
  SRCE_ERR_TYP_ID,
  Terr_Cd,
  Timestamp,
  Dw_File_Id,
  "OTI_DCRECD_0138" AS ERR_CD
FROM
  {{ source('MacD_GSCV_Dev','JS_IN_GSCV_DCReceipts_SCIPS_csv_tFileInputDelimited_11') }}
WHERE UPPER(localflag) LIKE '%Y%'
),
Schn_oti AS (
select 
  schn_oti_objt_dq_rule_assc_id,
  schn_dq_rule_id,
  xcpt_sevr_cd,
  err_msg_cat_tx
from {{ source('data_audit','schn_oti_objt_dq_rule_assc') }}
),
JS_IN_REJ_DELIM AS (
SELECT
  'MD5' AS oti_dc_rcpt_rjct_id,
  issue_code AS err_cd,
  wrin AS item_number,
  wsi,
  dc,
  countrycode,
  transdc,
  CAST(daterec AS DATE) AS daterc,
  cases,
  IF(
    CONTAINS_SUBSTR(File_na, 'GSCV_DCReceipts_ILOS') AND 
    CONTAINS_SUBSTR(fcacost, '.'),
    REPLACE(fcacost, '.', ','),
    fcacost
  ) AS fcacost,
  fcacurr,
  IF(
    CONTAINS_SUBSTR(File_na, 'GSCV_DCReceipts_ILOS') AND 
    CONTAINS_SUBSTR(dccost, '.'),
    REPLACE(dccost, '.', ','),
    dccost
  ) AS freedc,
  dccurr AS freedccurr,
  poline,
  ponum,
  CAST(transdate AS DATE) AS transdate,
  localflag,
  gtin,
  transdc_gln,
  facility_gln,
  dc_gln,
  IFNULL(SAFE_CAST(Terr_Cd AS INT64), 0) AS terr_cd,
  PARSE_TIMESTAMP('%Y%m%d%H%M%S', SUBSTR(Timestamp, 1, 14)) AS srce_file_recv_ts,
  CAST(recorded_date AS TIMESTAMP) AS fst_occr_dt,
  CAST(latest_reoccured_date AS TIMESTAMP) AS ltst_reoc_dt,
  IFNULL(count_of_occurance, 0) AS cnt_of_occr_nu,
  CAST(solved_date AS TIMESTAMP) AS rsol_dt,
  severity AS sevr_typ,
  error_description AS err_ds,
  comments AS cmnt_tx,
  0 AS rec_serl_nu,
  File_na AS srce_file_nm,
  IFNULL(DW_FILE_ID, '0') AS dw_file_id,
  SRCE_ERR_TYP_ID,
  NULL AS DQ_RSPN_TYP_ID
FROM {{ source('MacD_GSCV_Dev','JS_IN_REJ_DELIM_tFileInputDelimited_1') }}
WHERE localflag IS NULL OR localflag = ''
),
tUniqRow_1 AS (
  SELECT *,
  ROW_NUMBER() OVER (PARTITION BY
           err_cd,item_number,wsi,dc,countrycode,transdc,daterc,cases,fcacost,fcacurr,
           freedc,freedccurr,poline,ponum,transdate,localflag,gtin,transdc_gln,
           facility_gln,dc_gln,terr_cd
      ) AS rn
      FROM JS_IN_REJ_DELIM
)

SELECT DISTINCT
  "MD5" AS oti_dc_rcpt_rjct_id,
  Local_Items.ERR_CD AS err_cd,
  Local_Items.wrin AS item_number,
  Local_Items.wsi AS wsi,
  Local_Items.dc AS dc,
  Local_Items.countrycode AS countrycode,
  Local_Items.transdc AS transdc,
  CAST(Local_Items.daterec AS DATE) AS daterc,
  Local_Items.cases AS cases,
  IF(
    CONTAINS_SUBSTR(File_Attrb.dw_file_na, "GSCV_DCReceipts_ILOS") AND 
    CONTAINS_SUBSTR(Local_Items.fcacost, "."),
    REPLACE(Local_Items.fcacost, ".", ","),
    Local_Items.fcacost
  ) AS fcacost,
  Local_Items.fcacurr AS fcacurr,
  IF(
    CONTAINS_SUBSTR(File_Attrb.dw_file_na, "GSCV_DCReceipts_ILOS") AND 
    CONTAINS_SUBSTR(Local_Items.dccost, "."),
    REPLACE(Local_Items.dccost, ".", ","),
    Local_Items.dccost
  ) AS freedc,
  Local_Items.dccurr AS freedccurr,
  Local_Items.poline AS poline,
  Local_Items.ponum AS ponum,
  CAST(Local_Items.transdate AS DATE) AS transdate,
  Local_Items.localflag AS localflag,
  Local_Items.GTIN AS gtin,
  Local_Items.TransDC_GLN AS transdc_gln,
  Local_Items.DC_GLN_ID AS facility_gln,
  Local_Items.DC_GLN AS dc_gln,
  IFNULL(CAST(Local_Items.Terr_Cd AS INT64), 0) AS terr_cd,
  PARSE_TIMESTAMP('%Y%m%d%H%M%S', SUBSTR(Local_Items.Timestamp, 1, 14)) AS srce_file_recv_ts,
  TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), SECOND) AS fst_occr_dt,
  TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), SECOND) AS ltst_reoc_dt,
  1 AS cnt_of_occr_nu,
  CAST(NULL AS TIMESTAMP) AS rsol_dt,
  Schn_oti.xcpt_sevr_cd AS sevr_typ,
  Schn_oti.err_msg_cat_tx AS err_ds,
  CAST(NULL AS STRING) AS cmnt_tx,
  0 AS rec_serl_nu,
  File_Attrb.dw_file_na AS srce_file_nm,
  IFNULL(Local_Items.Dw_File_Id, "0") AS dw_file_id,
  Local_Items.SRCE_ERR_TYP_ID AS SRCE_ERR_TYP_ID,
  NULL AS DQ_RSPN_TYP_ID
FROM Local_Items
LEFT JOIN Schn_oti
  ON Schn_oti.schn_dq_rule_id = Local_Items.ERR_CD
LEFT JOIN {{ source('MacD_GSCV_Dev','JS_IN_FILE_ATTRB_tFileInputDelimited_6') }} File_Attrb
  ON File_Attrb.dw_file_id = Local_Items.Dw_File_Id

UNION ALL

  SELECT DISTINCT
    "MD5" AS oti_dc_rcpt_rjct_id, 
    li.ERR_CD AS err_cd,
    li.wrin AS item_number,
    li.wsi,
    li.dc,
    li.countrycode,
    li.transdc,
    CAST(li.daterec AS DATE) AS daterc,
    li.cases,
    CASE
      WHEN fl.dw_file_na LIKE '%GSCV_DCReceipts_ILOS%' AND li.fcacost LIKE '%.%'
      THEN REPLACE(li.fcacost, '.', ',')
      ELSE li.fcacost
    END AS fcacost,
    li.fcacurr,
    CASE
      WHEN fl.dw_file_na LIKE '%GSCV_DCReceipts_ILOS%' AND li.dccost LIKE '%.%'
      THEN REPLACE(li.dccost, '.', ',')
      ELSE li.dccost
    END AS freedc,
    li.dccurr AS freedccurr,
    li.poline,
    li.ponum,
    CAST(li.transdate AS DATE) AS transdate,
    li.localflag,
    li.GTIN,
    li.TransDC_GLN AS transdc_gln,
    li.DC_GLN_ID AS facility_gln,
    li.DC_GLN AS dc_gln,
    SAFE_CAST(NULLIF(li.Terr_Cd, '') AS INT64) AS terr_cd,
    PARSE_TIMESTAMP('%Y%m%d%H%M%S', SUBSTR(li.Timestamp, 1, 14)) AS srce_file_recv_ts,
    TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), SECOND) AS fst_occr_dt,
    TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), SECOND) AS ltst_reoc_dt,
    1 AS cnt_of_occr_nu,
    CAST(NULL AS TIMESTAMP) AS rsol_dt,
    so.xcpt_sevr_cd AS sevr_typ,
    so.err_msg_cat_tx AS err_ds,
    CAST(NULL AS STRING) AS cmnt_tx,
    0 AS rec_serl_nu,
    fl.dw_file_na AS srce_file_nm,
    IFNULL(li.Dw_File_Id, '0') AS dw_file_id,
    li.SRCE_ERR_TYP_ID,
    NULL AS DQ_RSPN_TYP_ID
  FROM Local_Item li
  LEFT JOIN Schn_oti so
    ON li.ERR_CD = so.schn_dq_rule_id
  LEFT JOIN {{ source('MacD_GSCV_Dev','JS_IN_FILE_ATTRB_tFileInputDelimited_6') }} fl
    ON li.Dw_File_Id = fl.dw_file_id

UNION ALL

SELECT * EXCEPT(rn) FROM tUniqRow_1 WHERE rn = 1