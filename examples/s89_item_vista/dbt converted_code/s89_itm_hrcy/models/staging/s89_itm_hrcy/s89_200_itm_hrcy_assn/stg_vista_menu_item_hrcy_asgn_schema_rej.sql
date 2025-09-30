{{ config(
    materialized='table',
    tags = ['s89_itm_hrcy_grandmaster','s89_200_itm_hrcy_assn','s89_200_itm_hrcy_assn_staging']
) }}

SELECT
TERRITORY_CODE ,
SMI_ID,
GLOBAL_MENU_IDENTIFIER,
HIERARCHY_NAME,
HIERARCHY_NODE_ID,
EFFECTIVE_DATE,
DW_FILE_ID,
error_code,
error_message
FROM {{ ref('stg_schema_compliance_check_vista_menu_item_hrchy_assn') }}
where error_message is not null