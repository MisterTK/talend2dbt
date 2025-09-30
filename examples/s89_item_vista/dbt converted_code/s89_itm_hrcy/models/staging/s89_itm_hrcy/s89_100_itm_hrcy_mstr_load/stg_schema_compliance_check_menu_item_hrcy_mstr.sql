{{ config(
    tags = ['s89_itm_hrcy_grandmaster','s89_100_itm_hrcy_mstr_load', 's89_100_itm_hrcy_mstr_load_staging'],
    materialized='table'
) }}

{% set start_insert_ts = var('insert_ts', none) %}

select HIERARCHY_NAME, 
TERRITORY_CODE, 
HIERARCHY_NODE_ID, 
HIERARCHY_NODE_LEVEL, 
HIERARCHY_PARENT_NODE_ID, 
NODE_NAME, 
HIERARCHY_EFFECTIVE_DATE, 
NODE_EFFECTIVE_DATE, 
HIERARCHY_END_DATE, 
DW_FILE_ID, 
INSERT_TS,
  {{ validate_schema_for_relation(source('MCD_DBT_IV', 'VISTA_Menu_Item_Hrcy_Mstr_IM2'), 'VISTA_Menu_Item_Hrcy_Mstr_IM2') }}
from {{ source('MCD_DBT_IV', 'VISTA_Menu_Item_Hrcy_Mstr_IM2') }}
where (
      {% if start_insert_ts %}
                INSERT_TS > '{{ start_insert_ts }}'
      {% else %}
      INSERT_TS > (
        select coalesce(MAX(INSERT_TS), cast('1900-01-01' as timestamp))
        from {{ source('MCD_DBT_IV', 'intermediate_vista_menu_item_hrcy_mstr_valid') }}
      )
      {% endif %}
)