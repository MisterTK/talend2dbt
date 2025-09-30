{{ config(
    materialized='table',
    tags = ['s89_itm_hrcy_grandmaster','s89_200_itm_hrcy_assn','s89_200_itm_hrcy_assn_staging'],
    schema='item_vista'
) }}

select 
	TERRITORY_CODE, 
	SMI_ID, 
	GLOBAL_MENU_IDENTIFIER, 
	HIERARCHY_NAME, 
	HIERARCHY_NODE_ID, 
	EFFECTIVE_DATE, 
	DW_FILE_ID,
	{{ validate_schema_for_relation(source('Item_Vista_Schema', 'VISTA_Menu_Item_Hrchy_Assn_tFileInputDelimited_3'), 'VISTA_Menu_Item_Hrchy_Assn_tFileInputDelimited_3') }}
from {{ source('Item_Vista_Schema', 'VISTA_Menu_Item_Hrchy_Assn_tFileInputDelimited_3') }}