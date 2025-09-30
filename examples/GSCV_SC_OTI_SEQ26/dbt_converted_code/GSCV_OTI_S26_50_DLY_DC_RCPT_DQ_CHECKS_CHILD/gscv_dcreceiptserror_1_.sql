{{
    config(
        materialized = 'table',
        tags = ['gscv_oti_s26_dcreceipts_grandmaster','gscv_oti_s26_50_dly_dc_rcpt_dq_checks_child'],
        alias = 'gscv_dcreceiptserror_1_'
    )
}}

WITH DC_RCPT_Error_In_1 AS (
select 
       item_number AS ItemNumber,
       wsi,
       dc,
       countrycode,
       transdc,
       daterc AS daterec,
       cases,
       fcacost,
       fcacurr,
       freedc,
       freedccurr,
	     ponum,
       poline,
       transdate,
       sevr_typ AS Error_Severity,
       err_cd AS Error_Code,
       err_ds AS Error_Descr
      from  `dmgcp-del-155.rmdw_stge.oti_dc_rcpt_rjct`
WHERE DATE(updt_dw_audt_ts) = CURRENT_DATE()
and err_cd like 'OTI_DCRECD_%'
and srce_file_nm not like 'GSCV_DCReceipts_SCIPS_%'
order by err_cd
)
SELECT DISTINCT
  d.ItemNumber,
  d.wsi,
  d.dc,
  d.countrycode,
  d.transdc,
  d.daterec,
  d.cases,
  d.fcacost,
  d.fcacurr,
  d.freedc,
  d.freedccurr,
  d.ponum,
  d.poline,
  d.transdate,
  d.Error_Severity,
  d.Error_Code,
  h.HAVI_Error_Description AS Error_Descr
FROM
  DC_RCPT_Error_In_1 AS d
INNER JOIN
  `dmgcp-del-155.MacD_GSCV_Dev.Lkp_HAVI_OTI_IDQ_Mapping_File_tFileInputDelimited_4` AS h
ON
  d.Error_Descr = h.OTI_IDQ_Error_Description
WHERE
  h.HAVI_Error_Description IS NOT NULL
  AND h.HAVI_Error_Description != ''
