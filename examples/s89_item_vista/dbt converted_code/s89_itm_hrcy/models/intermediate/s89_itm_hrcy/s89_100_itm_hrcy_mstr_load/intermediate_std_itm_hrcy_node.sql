
{{ config(
    materialized='table',
    tags = ['s89_itm_hrcy_grandmaster','s89_100_itm_hrcy_mstr_load', 's89_100_itm_hrcy_mstr_load_intermediate']
) }}

SELECT
    ITM_HRCY_NODE_ID,
    ITM_HRCY_NODE_NA,
    ITM_HRCY_NODE_EFF_DT,
    ITM_HRCY_NODE_END_DT,
    DW_FILE_ID
FROM
    {{ ref('intermediate_std_itm_hrcy_node_tuniq') }}
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY ITM_HRCY_NODE_ID
    ORDER BY ITM_HRCY_NODE_EFF_DT DESC, DW_FILE_ID DESC
) = 1