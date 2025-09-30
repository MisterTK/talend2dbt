{{ config(
    materialized='table',
    tags = ['s89_itm_hrcy_grandmaster','s89_200_itm_hrcy_assn','s89_200_itm_hrcy_assn_intermediate']
) }}

SELECT
    TERRITORY_CODE,
    SMI_ID,
    GLOBAL_MENU_IDENTIFIER,
    HIERARCHY_NAME,
    HIERARCHY_NODE_ID,
    EFFECTIVE_DATE,
    DW_FILE_ID
FROM
    {{ ref('intermediate_vista_menu_item_hrchy_assn_valid') }} 
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY
        TERRITORY_CODE,
        SMI_ID
    ORDER BY
        EFFECTIVE_DATE DESC,
        DW_FILE_ID DESC 
) > 1