{{ config(
    materialized='table',
    alias='hash_out_rest_rept_mo_sls',
    schema='RMDW_INT',
    tags=['glma_s19_wk_mo_agg_wk_agg_grandmaster','glma_s19_wk_mo_agg_110_generate_context']
) }}


SELECT
    c.CAL_DT,
    c.YR_NU,
    c.MO_NU,
    c.DY_OF_CAL_WK_NU,
    c.WK_END_THU_ID_NU,
    s.MAX_SLTC_DT
FROM {{ ref('caldt_valid') }} c
INNER JOIN {{ ref('restreptmosls_unique') }} s
    ON c.DUMMY_KEY = s.DUMMY_KEY
