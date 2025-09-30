{{
    config(
        materialized = 'incremental',
        tags = ['gscv_oti_s26_dcreceipts_grandmaster','gscv_oti_s26_100_arthur_dly_dc_rcpt_child'],
        alias = 'gscv_dly_dc_rcpt_dly_xchg_rate_rejects'
    )
}}
-- done
WITH Dly_Dc_Rcpt_Valid AS (
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
  Terr_Cd,
  Dw_File_Id,
  CASE
    WHEN LENGTH(Timestamp) > 14 THEN PARSE_TIMESTAMP('%Y%m%d%H%M%S%f', Timestamp)
    ELSE PARSE_TIMESTAMP('%Y%m%d%H%M%S', Timestamp)
  END AS Timestamp,
  SRCE_ERR_TYP_ID,
  CAST(NULL AS INT64) AS DQ_RSPN_TYP_ID
FROM
  {{ source('MacD_GSCV_Dev','JS_IN_GSCV_DCReceipts_ILOS_csv_tFileInputDelimited_1') }}
),
Shpg_Unt AS (
SELECT 
    SHPG_UNT_NU,
	XTRN_RAW_ITM_NU
FROM {{ source('rmdw_tables','shpg_unt') }}
WHERE EXTRACT(YEAR FROM shpg_unt_end_dt) = 9999 AND terr_cd=0
),
Dly_Dc_Rcpt_Valid_1 AS (
SELECT DISTINCT
    a.wrin,
    SAFE_CAST(a.dc AS INT64) AS dc_fcil_id,
    SAFE_CAST(a.countrycode AS INT64) AS dc_terr_cd,
    CASE
        WHEN LENGTH(a.transdate) = 7 THEN SAFE.PARSE_DATE('%d%m%Y', CONCAT('0', a.transdate))
        ELSE SAFE.PARSE_DATE('%d%m%Y', a.transdate)
    END AS dc_rcpt_xtrc_dt,
    a.ponum AS po_id,
    SAFE_CAST(a.poline AS INT64) AS po_ln_id,
    SAFE_CAST(a.wsi AS INT64) AS wsi_nu,
    b.SHPG_UNT_NU AS shpg_unt_nu,
    c.curn_iso_nu AS fca_lcl_curn_iso_nu,
    d.curn_iso_nu AS fdc_lcl_curn_iso_nu,
    CASE
        WHEN LENGTH(a.daterec) = 7 THEN SAFE.PARSE_DATE('%d%m%Y', CONCAT('0', a.daterec))
        ELSE SAFE.PARSE_DATE('%d%m%Y', a.daterec)
    END AS dc_rcpt_dt,
    SAFE_CAST(a.transdc AS INT64) AS dc_trsf_fcil_id,
    SAFE_CAST(a.cases AS INT64) AS shpg_unt_case_recv_qt,
    CASE
        WHEN a.fcacost IS NULL OR a.fcacost = '' THEN NULL
        WHEN CONTAINS_SUBSTR(a.fcacost, ',') THEN SAFE_CAST(REPLACE(a.fcacost, ',', '.') AS BIGNUMERIC)
        ELSE SAFE_CAST(a.fcacost AS BIGNUMERIC)
    END AS fca_spnd_am,
    CASE
        WHEN a.dccost IS NULL OR a.dccost = '' THEN NULL
        WHEN CONTAINS_SUBSTR(a.dccost, ',') THEN SAFE_CAST(REPLACE(a.dccost, ',', '.') AS BIGNUMERIC)
        ELSE SAFE_CAST(a.dccost AS BIGNUMERIC)
    END AS fdc_spnd_am,
    CASE
        WHEN a.localflag IS NULL OR a.localflag = '' THEN 0
        WHEN UPPER(SUBSTR(a.localflag, 1, 1)) = 'N' THEN 0
        WHEN UPPER(SUBSTR(a.localflag, 1, 1)) = 'Y' THEN 1
        ELSE 0
    END AS raw_itm_lcl_fl,
    840 AS gbal_curn_iso_nu,
    a.Dw_File_Id AS dw_file_id,
    a.Timestamp AS Timestamp,
    a.DC_GLN AS dc_gln_id,
    CAST(NULL AS STRING) AS dc_trsf_fcil_gln_id,
    a.DC_GLN_ID AS fcil_gln_id,
    a.GTIN AS gbal_trad_itm_nu,
    a.fcacurr AS fcacurr,
    a.dccurr AS dccurr,
    CAST(a.Terr_Cd AS INT64) AS Terr_Cd,
    a.localflag AS localflag,
    a.TransDC_GLN AS TransDC_GLN,
    a.fcacost AS fcacost,
    a.dccost AS dccost,
    CAST(a.SRCE_ERR_TYP_ID AS INT64) AS SRCE_ERR_TYP_ID,
    CAST(a.DQ_RSPN_TYP_ID AS INT64) AS DQ_RSPN_TYP_ID
FROM
    Dly_Dc_Rcpt_Valid AS a
INNER JOIN
    Shpg_Unt AS b
    ON REGEXP_REPLACE(a.wrin, '^0+(?!$)', '') = b.XTRN_RAW_ITM_NU
INNER JOIN
    {{ ref('js_out_gscv_curn') }} AS c
    ON a.fcacurr = c.curn_iso3_abbr_cd
INNER JOIN
    {{ ref('js_out_gscv_curn') }} AS d
    ON a.dccurr = d.curn_iso3_abbr_cd
),
-- Reject Output: Records missing currency rate(s)
DLY_CURN_XCNG_Reject AS (
SELECT
  t1.wrin,
  t1.dc_fcil_id,
  t1.dc_terr_cd,
  FORMAT_DATE('%d%m%Y', t1.dc_rcpt_xtrc_dt) AS dc_rcpt_xtrc_dt,
  t1.po_id,
  t1.po_ln_id,
  t1.wsi_nu,
  t1.shpg_unt_nu,
  t1.fca_lcl_curn_iso_nu,
  t1.fdc_lcl_curn_iso_nu,
  FORMAT_DATE('%d%m%Y', t1.dc_rcpt_dt) AS dc_rcpt_dt,
  t1.dc_trsf_fcil_id,
  t1.shpg_unt_case_recv_qt,
  t1.fcacost AS fca_spnd_am,
  t1.dccost AS fdc_spnd_am,
  t1.localflag AS raw_itm_lcl_fl,
  t1.fca_spnd_am AS gbal_fca_case_cost_am,
  t1.fdc_spnd_am AS gbal_fdc_case_cost_am,
  t1.gbal_curn_iso_nu,
  t1.dw_file_id,
  t1.dc_gln_id,
  t1.TransDC_GLN AS dc_trsf_fcil_gln_id,
  t1.fcil_gln_id,
  NULL AS dc_pri_gln_id,
  NULL AS dc_pri_trsf_fcil_gln_id,
  NULL AS fcil_pri_gln_id,
  t1.gbal_trad_itm_nu,
  NULL AS euro_fca_case_cost_am,
  NULL AS euro_fdc_case_cost_am,
  NULL AS euro_curn_iso_nu,
  t1.Timestamp,
  t1.Terr_Cd AS terr_cd_mapped,
  'OTI_DCRECD_0136' AS err_cd,
  t1.fcacurr,
  t1.dccurr,
  t1.SRCE_ERR_TYP_ID,
  t1.DQ_RSPN_TYP_ID
FROM
 Dly_Dc_Rcpt_Valid_1 AS t1
LEFT JOIN
  {{ ref('js_out_dly_curn_xcng_rate') }} AS t2
ON
  t1.dc_rcpt_dt = t2.cal_dt
  AND t1.fca_lcl_curn_iso_nu = t2.from_curn_iso_nu
LEFT JOIN
  {{ ref('js_out_dly_curn_xcng_rate') }} AS t3
ON
  t1.dc_rcpt_dt = t3.cal_dt
  AND t1.fdc_lcl_curn_iso_nu = t3.from_curn_iso_nu
WHERE
  t2.cal_dt IS NULL OR t3.cal_dt IS NULL
),
Dly_Dc_Rcpt_Valid_2 AS (
SELECT
  t1.wrin,
  t1.dc_fcil_id,
  t1.dc_terr_cd,
  t1.dc_rcpt_xtrc_dt,
  t1.po_id,
  t1.po_ln_id,
  t1.wsi_nu,
  t1.shpg_unt_nu,
  t1.fca_lcl_curn_iso_nu,
  t1.fdc_lcl_curn_iso_nu,
  t1.dc_rcpt_dt,
  t1.dc_trsf_fcil_id,
  t1.shpg_unt_case_recv_qt,
  t1.fca_spnd_am AS fca_case_cost_am,
  t1.fdc_spnd_am AS fdc_case_cost_am,
  t1.raw_itm_lcl_fl,
  -- Currency conversion calculation for gbal_fca_case_cost_am
  IFNULL(t2.dly_xcng_rate_nu, 0.00) * IFNULL(t1.fca_spnd_am, 0.00) AS gbal_fca_case_cost_am,
  -- Currency conversion calculation for gbal_fdc_case_cost_am
  IFNULL(t3.dly_xcng_rate_nu, 0.00) * IFNULL(t1.fdc_spnd_am, 0.00) AS gbal_fdc_case_cost_am,
  t1.gbal_curn_iso_nu,
  t1.dw_file_id,
  t1.dc_gln_id,
  t1.dc_trsf_fcil_gln_id,
  t1.fcil_gln_id,
  NULL AS dc_pri_gln_id,
  NULL AS dc_pri_trsf_fcil_gln_id,
  NULL AS fcil_pri_gln_id,
  t1.gbal_trad_itm_nu,
  t1.Timestamp,
  -- Conditional mapping for terr_cd_mapped
  CASE
    WHEN t1.raw_itm_lcl_fl = 1 THEN t1.dc_terr_cd
    ELSE 0
  END AS terr_cd_mapped,
  t1.fcacurr,
  t1.dccurr,
  t1.Terr_Cd,
  t1.localflag,
  t1.TransDC_GLN,
  t1.fcacost,
  t1.dccost,
  t1.SRCE_ERR_TYP_ID,
  t1.DQ_RSPN_TYP_ID
FROM
  Dly_Dc_Rcpt_Valid_1 AS t1
INNER JOIN
  {{ ref('js_out_dly_curn_xcng_rate') }} AS t2
ON
  t1.dc_rcpt_dt = t2.cal_dt
  AND t1.fca_lcl_curn_iso_nu = t2.from_curn_iso_nu
INNER JOIN
  {{ ref('js_out_dly_curn_xcng_rate') }} AS t3
ON
  t1.dc_rcpt_dt = t3.cal_dt
  AND t1.fdc_lcl_curn_iso_nu = t3.from_curn_iso_nu
),
DLY_CURN_XCNG_Reject_1 AS (
  SELECT
  t1.wrin,
  t1.dc_fcil_id,
  t1.dc_terr_cd,
  FORMAT_DATE('%d%m%Y', t1.dc_rcpt_xtrc_dt) AS dc_rcpt_xtrc_dt,
  t1.po_id,
  t1.po_ln_id,
  t1.wsi_nu,
  t1.shpg_unt_nu,
  t1.fca_lcl_curn_iso_nu,
  t1.fdc_lcl_curn_iso_nu,
  FORMAT_DATE('%d%m%Y', t1.dc_rcpt_dt) AS dc_rcpt_dt,
  t1.dc_trsf_fcil_id,
  t1.shpg_unt_case_recv_qt,
  CAST(t1.fcacost AS STRING) AS fca_case_cost_am,
  CAST(t1.dccost AS STRING) AS fdc_case_cost_am,
  CAST(t1.localflag AS STRING) AS raw_itm_lcl_fl,
  t1.fca_case_cost_am AS gbal_fca_case_cost_am,
  t1.fdc_case_cost_am AS gbal_fdc_case_cost_am,
  t1.gbal_curn_iso_nu,
  t1.dw_file_id,
  t1.dc_gln_id,
  t1.TransDC_GLN AS dc_trsf_fcil_gln_id,
  t1.fcil_gln_id,
  t1.dc_pri_gln_id,
  t1.dc_pri_trsf_fcil_gln_id,
  t1.fcil_pri_gln_id,
  t1.gbal_trad_itm_nu,
  NULL AS euro_fca_case_cost_am,
  NULL AS euro_fdc_case_cost_am,
  NULL AS euro_curn_iso_nu,
  t1.Timestamp,
  t1.Terr_Cd AS terr_cd_mapped,
  'OTI_DCRECD_0136' AS err_cd,
  t1.fcacurr,
  t1.dccurr,
  t1.SRCE_ERR_TYP_ID,
  t1.DQ_RSPN_TYP_ID
FROM
  Dly_Dc_Rcpt_Valid_2 AS t1
LEFT JOIN
  {{ ref('js_out_dly_curn_xcng_rate') }} AS t2
ON
  t1.dc_rcpt_dt = t2.cal_dt
  AND t2.from_curn_iso_nu = 978
WHERE
  t2.cal_dt IS NULL
)
SELECT * FROM DLY_CURN_XCNG_Reject 
UNION ALL
SELECT * FROM DLY_CURN_XCNG_Reject_1