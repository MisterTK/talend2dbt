{{ config(
    materialized='ephemeral', 
    tags = ['s89_itm_hrcy_grandmaster','s89_100_itm_hrcy_mstr_load', 's89_100_itm_hrcy_mstr_load_intermediate']
) }}

SELECT
    TERRITORY_CODE AS TERR_CD,
    HIERARCHY_NODE_ID AS ITM_HRCY_NODE_ID,
    HIERARCHY_PARENT_NODE_ID AS ITM_HRCY_PRNT_NODE_ID,
    HIERARCHY_NODE_LEVEL AS ITM_HRCY_LVL_NU,
    CASE
        WHEN HIERARCHY_EFFECTIVE_DATE IS NULL OR TRIM(CAST(HIERARCHY_EFFECTIVE_DATE AS STRING)) = '' THEN NULL
        ELSE CAST(HIERARCHY_EFFECTIVE_DATE AS DATE)
    END AS ITM_HRCY_LVL_EFF_DT,
    NODE_EFFECTIVE_DATE AS NODE_EFFECTIVE_DATE,
    DW_FILE_ID AS DW_FILE_ID
FROM
    {{ ref('intermediate_vista_menu_item_hrcy_mstr_valid') }}