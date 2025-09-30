{{ config(
    materialized='table',
    alias='cal_dt_2',
    schema='RMDW_INT',
    tags=['glma_s19_wk_mo_agg_wk_agg_grandmaster','glma_s19_wk_mo_agg_110_generate_context']  
) }}

SELECT
    CAL_DT,
    WK_END_THU_ID_NU
FROM
    {{ source('RMDW_RAW', 'CAL_DT') }}
WHERE
    DY_OF_CAL_WK_NU = 6