{{ config(materialized='table',
    alias='hash_out_max_thu_dt',
    schema='RMDW_INT',
    tags=['glma_s19_wk_mo_agg_wk_agg_grandmaster','glma_s19_wk_mo_agg_110_generate_context']

) }}
    

 with sorted_data AS (
    SELECT
        CALC_WEEK_CAL_DT,
        CALC_WEEK_WK_END_THU_ID_NU,
        DUMMY_KEY,
        ROW_NUMBER() OVER (
            PARTITION BY DUMMY_KEY
            ORDER BY
                DUMMY_KEY ASC,
                CALC_WEEK_CAL_DT DESC
        ) AS row_num
    FROM
        {{ ref('maxthudt') }}
)

SELECT
    CALC_WEEK_CAL_DT,
    CALC_WEEK_WK_END_THU_ID_NU,
    DUMMY_KEY
FROM
    sorted_data
WHERE
    row_num = 1