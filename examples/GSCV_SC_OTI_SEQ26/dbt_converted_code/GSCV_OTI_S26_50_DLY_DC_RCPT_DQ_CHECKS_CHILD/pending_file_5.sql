{{
    config(
        materialized = 'ephemeral',
        tags = ['gscv_oti_s26_dcreceipts_grandmaster','gscv_oti_s26_50_dly_dc_rcpt_dq_checks_child'],
        alias = 'temp'
    )
}}

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
FROM `dmgcp-del-155.MacD_GSCV_Dev.JS_IN_GSCV_DCReceipts_ILOS_csv_tFileInputDelimited_5`
WHERE UPPER(localflag) LIKE "%Y%"
