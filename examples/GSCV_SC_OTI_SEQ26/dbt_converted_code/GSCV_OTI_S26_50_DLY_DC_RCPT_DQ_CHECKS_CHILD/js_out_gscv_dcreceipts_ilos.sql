{{
    config(
        materialized = 'table',
        tags = ['gscv_oti_s26_dcreceipts_grandmaster','gscv_oti_s26_50_dly_dc_rcpt_dq_checks_child'],
        alias = 'js_out_gscv_dcreceipts_ilos'
    )
}}

{% set col_count = get_column_count('MacD_GSCV_Dev', 'JS_IN_GSCV_DCReceipts_ILOS_csv_tFileInputDelimited_2') | int %}
{% set v1_def_col_cnt = var('v1_def_col_cnt') | int %}

{% if col_count == v1_def_col_cnt %}
    SELECT
    REGEXP_REPLACE(t1.wrin, r'^0+(?!$)', '') AS wrin,
    REGEXP_REPLACE(t1.wsi, r'^0+(?!$)', '') AS wsi,
    REGEXP_REPLACE(t1.dc, r'^0+(?!$)', '') AS dc,
    t1.countrycode,
    REGEXP_REPLACE(t1.transdc, r'^0+(?!$)', '') AS transdc,
    t1.daterec,
    t1.cases,
--	   SAFE_CAST(REPLACE(t1.fcacost, ',', '.') AS NUMERIC) AS fcacost,
	'"' || IF(STRPOS(t1.fcacost, ',') > 0, REPLACE(t1.fcacost, ',', '.'), t1.fcacost) || '"' AS fcacost,
    t1.fcacurr,
--	   SAFE_CAST(REPLACE(t1.dccost, ',', '.') AS NUMERIC) AS dccost,
	'"' || IF(STRPOS(t1.dccost, ',') > 0, REPLACE(t1.dccost, ',', '.'), t1.dccost) || '"' AS dccost,
    t1.dccurr,
    t1.ponum,
    t1.poline,
    t1.transdate,
    CAST(NULL AS STRING) AS localflag,
    CAST(NULL AS STRING) AS GTIN,
    CAST(NULL AS STRING) AS DC_GLN,
    CAST(NULL AS STRING) AS TransDC_GLN,
    CAST(NULL AS STRING) AS DC_GLN_ID,
    CAST(t1.SRCE_ERR_TYP_ID AS INT64) AS SRCE_ERR_TYP_ID,
    t1.Terr_Cd,
    t1.Timestamp,
    t1.Dw_File_Id 
    FROM
        {{ source('MacD_GSCV_Dev','JS_IN_GSCV_DCReceipts_ILOS_csv_tFileInputDelimited_8') }} AS t1 
{% elif col_count != v1_def_col_cnt %}
    SELECT
    REGEXP_REPLACE(t1.wrin, r'^0+(?!$)', '') AS wrin,
    REGEXP_REPLACE(t1.wsi, r'^0+(?!$)', '') AS wsi,
    REGEXP_REPLACE(t1.dc, r'^0+(?!$)', '') AS dc,
    t1.countrycode,
    REGEXP_REPLACE(t1.transdc, r'^0+(?!$)', '') AS transdc,
    t1.daterec,
    t1.cases,
--	SAFE_CAST(REPLACE(t1.fcacost, ',', '.') AS NUMERIC) AS fcacost,
	'"' || IF(STRPOS(t1.fcacost, ',') > 0, REPLACE(t1.fcacost, ',', '.'), t1.fcacost) || '"' AS fcacost,
    t1.fcacurr,
--	SAFE_CAST(REPLACE(t1.dccost, ',', '.') AS NUMERIC) AS dccost,
	'"' || IF(STRPOS(t1.dccost, ',') > 0, REPLACE(t1.dccost, ',', '.'), t1.dccost) || '"' AS dccost,
    t1.dccurr,
    t1.ponum,
    t1.poline,
    t1.transdate,
    CAST(NULL AS STRING) AS localflag,
    CAST(NULL AS STRING) AS GTIN,
    CAST(NULL AS STRING) AS DC_GLN,
    CAST(NULL AS STRING) AS TransDC_GLN,
    CAST(NULL AS STRING) AS DC_GLN_ID,
    CAST(NULL AS INT64) AS SRCE_ERR_TYP_ID, -- Note: SRCE_ERR_TYP_ID has no expression in Talend.
    t1.Terr_Cd,
    t1.Timestamp,
    t1.Dw_File_Id
    FROM
        {{ source('MacD_GSCV_Dev','JS_IN_GSCV_DCReceipts_ILOS_csv_tFileInputDelimited_7') }} AS t1 
{% endif %}

