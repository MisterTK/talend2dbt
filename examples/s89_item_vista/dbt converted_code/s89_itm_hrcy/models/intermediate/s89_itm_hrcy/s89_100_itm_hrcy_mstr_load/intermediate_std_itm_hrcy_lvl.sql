
{{ config(
    materialized='table',
    tags = ['s89_itm_hrcy_grandmaster','s89_100_itm_hrcy_mstr_load', 's89_100_itm_hrcy_mstr_load_intermediate']
) }}

WITH ranked_data AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                ITM_HRCY_LVL_NU,
                ITM_HRCY_LVL_EFF_DT
            ORDER BY
                ITM_HRCY_LVL_DS ASC,
                ITM_HRCY_LVL_END_DT ASC,
                DW_FILE_ID ASC
        ) as rn
    FROM
        {{ ref('intermediate_std_itm_hrcy_lvl_tuniq') }}
)
SELECT
    ITM_HRCY_LVL_NU,
    ITM_HRCY_LVL_EFF_DT,
    ITM_HRCY_LVL_DS,
    ITM_HRCY_LVL_END_DT,
    DW_FILE_ID
FROM
    ranked_data
WHERE
    rn = 1 