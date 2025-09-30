{{ config(
    materialized='table',             
    alias='caldt_valid' ,
    schema='RMDW_INT',
    tags=['glma_s19_wk_mo_agg_wk_agg_grandmaster','glma_s19_wk_mo_agg_110_generate_context']        
) }}

WITH input_data AS (
    SELECT
         CAL_DT,
        CAST(YR_NU AS INT64) AS YR_NU,
        CAST(MO_NU AS INT64) AS MO_NU,
        CAST(DY_OF_CAL_WK_NU AS INT64) AS DY_OF_CAL_WK_NU,
        CAST(WK_END_THU_ID_NU AS INT64) AS WK_END_THU_ID_NU
    FROM {{ source('RMDW_RAW', 'CAL_DT') }}
)

SELECT
    CAL_DT,
    YR_NU,
    MO_NU,
    DY_OF_CAL_WK_NU,
    WK_END_THU_ID_NU,
    "X" AS DUMMY_KEY
FROM input_data
