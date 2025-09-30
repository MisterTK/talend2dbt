{{
    config(
        materialized = 'ephemeral',
        tags = ['gscv_oti_s26_dcreceipts_grandmaster','gscv_oti_s26_150_arthur_dly_dc_rcpt_child'],
        alias = 'gscv_dly_dc_rcpt_lookup_rejects_appended_2'
    )
}}

WITH Main1 AS (
SELECT
  row9.wrin,
  row9.wsi,
  row9.dc,
  row9.countrycode,
  row9.transdc,
  PARSE_DATE('%d%m%Y',
    CASE WHEN LENGTH(row9.daterec) = 7 THEN CONCAT('0', row9.daterec) ELSE row9.daterec END
  ) AS daterec,
  row9.cases,
  row9.fcacost,
  row9.fcacurr,
  row9.dccost,
  row9.dccurr,
  row9.ponum,
  row9.poline,
  PARSE_DATE('%d%m%Y',
    CASE WHEN LENGTH(row9.transdate) = 7 THEN CONCAT('0', row9.transdate) ELSE row9.transdate END
  ) AS transdate,
  row9.localflag,
  row9.GTIN,
  row9.DC_GLN,
  row9.TransDC_GLN,
  row9.DC_GLN_ID,
  row9.SRCE_ERR_TYP_ID,
  CAST(COALESCE(row9.Terr_Cd, '0') AS INT64) AS Terr_Cd,
  row9.Timestamp,
  row9.Dw_File_Id,
  REGEXP_REPLACE("", r'GSCV_DCReceipts__\w*_valid', '') AS File_na, -- need to check logic
  row9.daterec AS daterec_raw,
  row9.transdate AS transdate_raw
FROM {{ source('MacD_GSCV_Dev','JS_IN_GSCV_DCReceipts_ILOS_csv_tFileInputDelimited_1') }} row9
),
row14 AS (
    SELECT * FROM {{ ref('gscv_dly_dc_rcpt_lookup_rejects_appended_1') }}
    UNION ALL
    SELECT * FROM {{ source('MacD_GSCV_Dev','GSCV_DQ_OTI_reject_final_INSERT2_psv_tFileInputDelimited_2') }}
),
CriticalData AS (
    SELECT * FROM row14 ORDER BY sevr_typ ASC
),
row12 AS (
SELECT
  CriticalData.oti_dc_rcpt_rjct_id,
  CriticalData.err_cd,
  CriticalData.item_number,
  CriticalData.wsi,
  CriticalData.dc,
  CriticalData.countrycode,
  CriticalData.transdc,
  PARSE_DATE(
    '%d%m%Y',
    CASE
      WHEN LENGTH(CriticalData.daterc) = 7 THEN CONCAT('0', CriticalData.daterc)
      ELSE CriticalData.daterc
    END
  ) AS daterc,
  CriticalData.cases,
  CASE
    WHEN STRPOS(CriticalData.srce_file_nm, 'GSCV_DCReceipts_ILOS') > 0
         AND STRPOS(CriticalData.fcacost, '.') > 0
    THEN REGEXP_REPLACE(CriticalData.fcacost, r'\.', ',')
    ELSE CriticalData.fcacost
  END AS fcacost,
  CriticalData.fcacurr,
  CASE
    WHEN STRPOS(CriticalData.srce_file_nm, 'GSCV_DCReceipts_ILOS') > 0
         AND STRPOS(CriticalData.freedc, '.') > 0
    THEN REGEXP_REPLACE(CriticalData.freedc, r'\.', ',')
    ELSE CriticalData.freedc
  END AS freedc,
  CriticalData.freedccurr,
  CriticalData.poline,
  CriticalData.ponum,
  PARSE_DATE(
    '%d%m%Y',
    CASE
      WHEN LENGTH(CriticalData.transdate) = 7 THEN CONCAT('0', CriticalData.transdate)
      ELSE CriticalData.transdate
    END
  ) AS transdate,
  CriticalData.localflag,
  CriticalData.gtin,
  CriticalData.transdc_gln,
  CriticalData.facility_gln,
  CriticalData.dc_gln,
  CriticalData.terr_cd,
  CriticalData.srce_file_recv_ts,
  CriticalData.fst_occr_dt,
  CriticalData.ltst_reoc_dt,
  CriticalData.cnt_of_occr_nu,
  CriticalData.rsol_dt,
  CriticalData.sevr_typ,
  CriticalData.err_ds,
  CriticalData.cmnt_tx,
  CriticalData.rec_serl_nu,
  CriticalData.srce_file_nm,
  CriticalData.dw_file_id,
  CriticalData.SRCE_ERR_TYP_ID,
  CriticalData.DQ_RSPN_TYP_ID
FROM CriticalData AS CriticalData
),
Js_In_Rej_2 AS (
SELECT
  Main1.wrin AS wrin,
  Main1.wsi AS wsi,
  Main1.dc AS dc,
  Main1.countrycode AS countrycode,
  Main1.transdc AS transdc,
  Main1.daterec_raw AS daterec,
  Main1.cases AS cases,
  Main1.fcacost AS fcacost,
  Main1.fcacurr AS fcacurr,
  Main1.dccost AS dccost,
  Main1.dccurr AS dccurr,
  Main1.ponum AS ponum,
  Main1.poline AS poline,
  Main1.transdate_raw AS transdate,
  Main1.localflag AS localflag,
  Main1.GTIN AS GTIN,
  Main1.DC_GLN AS DC_GLN,
  Main1.TransDC_GLN AS TransDC_GLN,
  Main1.DC_GLN_ID AS DC_GLN_ID,
  Main1.SRCE_ERR_TYP_ID AS SRCE_ERR_TYP_ID,
  Main1.Terr_Cd AS Terr_Cd,
  Main1.Timestamp AS Timestamp,
  Main1.Dw_File_Id AS DW_FILE_ID,
  CURRENT_TIMESTAMP() AS recorded_date,
  CURRENT_TIMESTAMP() AS latest_reoccured_date,
  1 AS count_of_occurance,
  CAST(NULL AS DATE) AS solved_date,
  CAST(NULL AS STRING) AS severity,
  CAST(NULL AS STRING) AS error_description,
  CASE
    WHEN Main1.SRCE_ERR_TYP_ID = 1 THEN 'SRC_DCRECD_0101'
    WHEN Main1.SRCE_ERR_TYP_ID = 2 THEN 'SRC_DCRECD_0102'
    ELSE 'INVALID'
  END AS issue_code,
  CAST(NULL AS STRING) AS comments,
  CAST(NULL AS STRING) AS serial_number,
  Main1.File_na AS File_na
FROM Main1 AS Main1
LEFT JOIN row12 AS row12
  ON Main1.wrin = row12.item_number
),
schn_oti_objt_dq_rule_assc AS (
  SELECT  schn_oti_objt_na, schn_dq_rule_id,err_msg_cat_tx,xcpt_sevr_cd
  FROM `dmgcp-del-155.data_audit.schn_oti_objt_dq_rule_assc` 
),
Js_Out_Rej_2 AS (
SELECT
  -- surrogate key (Talend put "MD5" literal, probably you need MD5 of some combination, 
  -- but for now keeping as string "MD5")
  "MD5" AS oti_dc_rcpt_rjct_id,
  Js.issue_code AS err_cd,
  Js.wrin AS item_number,
  Js.wsi,
  Js.dc,
  Js.countrycode,
  Js.transdc,
  Js.daterec AS daterc,
  Js.cases,
  CASE
    WHEN Js.File_na LIKE '%GSCV_DCReceipts_ILOS%' AND Js.fcacost LIKE '%.%'
      THEN REPLACE(Js.fcacost, '.', ',')
    ELSE Js.fcacost
  END AS fcacost,
  Js.fcacurr,
  CASE
    WHEN Js.File_na LIKE '%GSCV_DCReceipts_ILOS%' AND Js.dccost LIKE '%.%'
      THEN REPLACE(Js.dccost, '.', ',')
    ELSE Js.dccost
  END AS freedc,
  Js.dccurr AS freedccurr,
  Js.poline,
  Js.ponum,
  Js.transdate,
  Js.localflag,
  Js.GTIN AS gtin,
  Js.TransDC_GLN AS transdc_gln,
  Js.DC_GLN_ID AS facility_gln,
  Js.DC_GLN AS dc_gln,
  Js.Terr_Cd AS terr_cd,
  SAFE.PARSE_TIMESTAMP('%Y%m%d%H%M%S', SUBSTR(Js.Timestamp, 1, 14)) AS srce_file_recv_ts,
  Js.recorded_date AS fst_occr_dt,
  Js.latest_reoccured_date AS ltst_reoc_dt,
  COALESCE(Js.count_of_occurance, 0) AS cnt_of_occr_nu,
  Js.solved_date AS rsol_dt,
  r15.xcpt_sevr_cd AS sevr_typ,
  r15.err_msg_cat_tx AS err_ds,
  Js.comments AS cmnt_tx,
  CAST(0 AS INT64) AS rec_serl_nu,
  Js.File_na AS srce_file_nm,
  COALESCE(Js.DW_FILE_ID, "0") AS dw_file_id,
  Js.SRCE_ERR_TYP_ID,
  CAST(NULL AS INT64) AS DQ_RSPN_TYP_ID
FROM Js_In_Rej_2 AS Js
LEFT JOIN schn_oti_objt_dq_rule_assc AS r15
  ON Js.issue_code = r15.schn_dq_rule_id
),
tUniqRow_2 AS (
  SELECT *,
        ROW_NUMBER() OVER (PARTITION BY err_cd, item_number, wsi, dc, countrycode,
        transdc, daterc, cases, fcacost, fcacurr, freedc, freedccurr, poline, ponum,
        transdate, localflag, gtin, transdc_gln, facility_gln, dc_gln, terr_cd , dw_file_id
      ) AS rn 
      FROM Js_Out_Rej_2
)
SELECT * EXCEPT(rn) FROM tUniqRow_2 WHERE rn = 1