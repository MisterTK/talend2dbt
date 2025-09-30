{{ config(
    materialized='table',
    tags = ['s89_itm_hrcy_grandmaster','s89_100_itm_hrcy_mstr_load', 's89_100_itm_hrcy_mstr_load_mart']
) }}

SELECT
    TERR_CD,
    ITM_HRCY_NODE_ID,
    ITM_HRCY_PRNT_NODE_ID,
    ITM_HRCY_LVL_NU,
    ITM_HRCY_LVL_EFF_DT,
    NODE_EFFECTIVE_DATE,
    DW_FILE_ID
FROM
    {{ ref('intermediate_hrcy_lookup_tuniq') }}
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY
        ITM_HRCY_NODE_ID,
        ITM_HRCY_PRNT_NODE_ID,
        ITM_HRCY_LVL_NU,
        ITM_HRCY_LVL_EFF_DT,
        NODE_EFFECTIVE_DATE
    ORDER BY
        DW_FILE_ID DESC
) = 1