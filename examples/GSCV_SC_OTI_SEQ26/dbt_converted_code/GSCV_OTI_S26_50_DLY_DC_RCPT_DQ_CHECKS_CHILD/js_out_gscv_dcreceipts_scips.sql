{{
    config(
        materialized = 'table',
        tags = ['gscv_oti_s26_dcreceipts_grandmaster','gscv_oti_s26_50_dly_dc_rcpt_dq_checks_child'],
        alias = 'js_out_gscv_dcreceipts_scips'
    )
}}

{% set col_count = get_column_count('MacD_GSCV_Dev', 'JS_IN_GSCV_DCReceipts_SCIPS_csv_tFileInputDelimited_3') | int %}
{% set v3_def_col_cnt = var('v3_def_col_cnt') | int %}

{% if col_count == v3_def_col_cnt %}
    SELECT
    REGEXP_REPLACE(wrin, r'^0+(?!$)', '') AS wrin,
    REGEXP_REPLACE(wsi, r'^0+(?!$)', '') AS wsi,
    REGEXP_REPLACE(dc, r'^0+(?!$)', '') AS dc,
    countrycode,
    REGEXP_REPLACE(transdc, r'^0+(?!$)', '') AS transdc,
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
    FROM
        {{ source('MacD_GSCV_Dev','JS_IN_GSCV_DCReceipts_SCIPS_csv_tFileInputDelimited_10') }}
{% elif col_count != v3_def_col_cnt %}
    SELECT
    REGEXP_REPLACE(wrin, r'^0+(?!$)', '') AS wrin,
    REGEXP_REPLACE(wsi, r'^0+(?!$)', '') AS wsi,
    REGEXP_REPLACE(dc, r'^0+(?!$)', '') AS dc,
    countrycode,
    REGEXP_REPLACE(transdc, r'^0+(?!$)', '') AS transdc,
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
    CAST(NULL AS INT64) AS SRCE_ERR_TYP_ID, -- Note: SRCE_ERR_TYP_ID has no expression in Talend.
    Terr_Cd,
    Timestamp,
    Dw_File_Id
    FROM
        {{ source('MacD_GSCV_Dev','JS_IN_GSCV_DCReceipts_SCIPS_csv_tFileInputDelimited_9') }}
{% endif %}