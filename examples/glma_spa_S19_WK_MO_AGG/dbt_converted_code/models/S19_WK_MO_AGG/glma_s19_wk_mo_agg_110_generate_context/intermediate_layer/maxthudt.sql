{{ config(materialized='table',
    alias='maxthudt',
    schema='RMDW_INT',
    tags=['glma_s19_wk_mo_agg_wk_agg_grandmaster','glma_s19_wk_mo_agg_110_generate_context']) }}

WITH base AS (
    SELECT 
        CAL_DT.CAL_DT AS CAL_DT,
        YR_NU,
        MO_NU,
        DY_OF_CAL_WK_NU,
        WK_END_THU_ID_NU,
        MAX_SLTC_DT
    FROM {{ ref('hash_out_rest_rept_mo_sls') }}
),

-- filter: CAL_DT < current_date and DY_OF_CAL_WK_NU = 5
filtered AS (
    SELECT
        CAL_DT AS CALC_WEEK_CAL_DT,
        WK_END_THU_ID_NU AS CALC_WEEK_WK_END_THU_ID_NU,
        "X" AS DUMMY_KEY
    FROM base
    WHERE CAL_DT < CURRENT_DATE()
      AND DY_OF_CAL_WK_NU = 5
)

SELECT * FROM filtered
