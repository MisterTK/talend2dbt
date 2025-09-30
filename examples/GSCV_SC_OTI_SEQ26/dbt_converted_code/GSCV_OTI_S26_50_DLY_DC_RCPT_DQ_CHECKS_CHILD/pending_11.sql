{{
    config(
        materialized = 'ephemeral',
        tags = ['gscv_oti_s26_dcreceipts_grandmaster','gscv_oti_s26_50_dly_dc_rcpt_dq_checks_child'],
        alias = 'temp_1'
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
  Dw_File_Id
FROM   {{ source('MacD_GSCV_Dev','JS_IN_GSCV_DCReceipts_SCIPS_csv_tFileInputDelimited_11') }} AS row5
WHERE localflag IS NULL OR localflag = ''