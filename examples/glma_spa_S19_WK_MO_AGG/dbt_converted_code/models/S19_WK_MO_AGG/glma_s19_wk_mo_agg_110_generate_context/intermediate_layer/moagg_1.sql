{{ config(
    materialized='table',
    alias='moagg_1',
    schema='RMDW_INT',
    tags=['glma_s19_wk_mo_agg_wk_agg_grandmaster','glma_s19_wk_mo_agg_110_generate_context']
) }}

SELECT
    PSV_USE_NUM_MONTHS_FL AS USE_NUM_MONTHS_FL,
    CAST(PSV_NUM_MONTHS AS INT64) AS NUM_MONTHS,
    PARSE_DATE('%Y-%m-%d', PSV_MONTH_FROM_DT) AS MONTH_FROM_DT,
    PARSE_DATE('%Y-%m-%d', PSV_MONTH_TO_DT) AS MONTH_TO_DT,
    PSV_MONTH_RUN_PMIX AS MONTH_RUN_PMIX,
    'N' AS MONTH_RUN_TM_SEG_SLS,
    PSV_MONTH_RUN_DYPT_SLS AS MONTH_RUN_DYPT_SLS,
    'X' AS DUMMY_KEY
FROM
    {{ source('RMDW_RAW','MO_AGGS') }}