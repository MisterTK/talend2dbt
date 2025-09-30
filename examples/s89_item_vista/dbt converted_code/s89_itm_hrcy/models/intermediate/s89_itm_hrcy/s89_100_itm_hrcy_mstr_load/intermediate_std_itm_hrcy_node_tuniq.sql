{{ config(
    materialized='ephemeral',
    tags = ['s89_itm_hrcy_grandmaster','s89_100_itm_hrcy_mstr_load', 's89_100_itm_hrcy_mstr_load_intermediate']
) }}

SELECT
    HIERARCHY_NODE_ID AS ITM_HRCY_NODE_ID,
    NODE_NAME AS ITM_HRCY_NODE_NA,
    CASE
        WHEN HIERARCHY_EFFECTIVE_DATE IS NULL OR TRIM(CAST(HIERARCHY_EFFECTIVE_DATE AS STRING)) = '' THEN NULL
        ELSE CAST(HIERARCHY_EFFECTIVE_DATE AS DATE)
    END AS ITM_HRCY_NODE_EFF_DT,
    CASE
        WHEN HIERARCHY_END_DATE IS NULL OR TRIM(CAST(HIERARCHY_END_DATE AS STRING)) = '' THEN NULL
        ELSE CAST(HIERARCHY_END_DATE AS DATE)
    END AS ITM_HRCY_NODE_END_DT,
    DW_FILE_ID AS DW_FILE_ID
FROM
    {{ ref('intermediate_vista_menu_item_hrcy_mstr_valid') }}