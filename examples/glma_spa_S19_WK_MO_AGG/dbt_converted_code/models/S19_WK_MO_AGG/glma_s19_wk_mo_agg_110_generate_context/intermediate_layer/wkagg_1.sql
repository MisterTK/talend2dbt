{{ config(
    materialized='table',
    alias='wk_agg_1',
    schema='RMDW_INT',
    tags=['glma_s19_wk_mo_agg_wk_agg_grandmaster','glma_s19_wk_mo_agg_110_generate_context']
) }}

SELECT
    PSV_USE_NUM_WEEKS_FL AS USE_NUM_WEEKS_FL,
    CAST(PSV_NUM_WEEKS AS INT64) AS NUM_WEEKS,
    PARSE_DATE('%Y-%m-%d', PSV_WEEK_FROM_DT) AS WEEK_FROM_DT,
    PARSE_DATE('%Y-%m-%d', PSV_WEEK_TO_DT) AS WEEK_TO_DT,
    PSV_WEEK_RUN_PMIX AS WEEK_RUN_PMIX,
    'N' AS WEEK_RUN_TM_SEG_SLS,
    PSV_WEEK_RUN_DYPT_SLS AS WEEK_RUN_DYPT_SLS,
    PSV_WEEK_RUN_SLS AS WEEK_RUN_SLS,
    'X' AS DUMMY_KEY
FROM
    {{source('RMDW_RAW','WK_AGG')}}