{{ config(
    materialized = 'table',
    database='prj-cp-dagdwstr-dev01',
    schema='rmdw_tables',
    alias = 'dcreceipts_processed_file_details',
    tags=['gscv_s34_grand_master', 'gscv_s34_100_dcreceipts_processed_files_extract_child','gscv_s34_100_dcreceipts_processed_files_extract_child_mart'],
) }}


SELECT
    js.dw_file_na,
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', js.file_mkt_ogin_ts) AS file_mkt_ogin_ts,
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', js.file_prcs_strt_ts) AS file_prcs_strt_ts,
    CAST(js.file_size_nu AS INT64) AS file_size_nu,
    st.dw_audt_stus_typ_ds,
    js.insert_ts
FROM {{ source('rmdw_stg','metedata_state_result') }} AS js
INNER JOIN {{ ref('dw_audt_stus_typ_ds') }} AS st
    ON js.dw_audt_stus_typ_id = st.dw_audt_stus_typ_id
WHERE
    js.dw_file_na IS NOT NULL
    AND js.dw_file_na != ''
    AND js.file_mkt_ogin_ts IS NOT NULL
    AND js.file_mkt_ogin_ts != ''
    AND js.file_prcs_strt_ts IS NOT NULL
    AND js.file_prcs_strt_ts != ''
    AND js.file_size_nu IS NOT NULL
    AND js.file_size_nu != ''
    AND st.dw_audt_stus_typ_ds IS NOT NULL
    AND st.dw_audt_stus_typ_ds != ''