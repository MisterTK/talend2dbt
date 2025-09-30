{{ config(
    materialized='table',
    tags = ['s89_itm_hrcy_grandmaster','s89_200_itm_hrcy_assn','s89_200_itm_hrcy_assn_intermediate']
) }}

with validated as (
  select * from {{ ref('stg_schema_compliance_check_vista_menu_item_hrchy_assn') }}
)

select
    {{ cast_columns(intermdiate_target_table_project='dmgcp-del-155', intermdiate_target_table_dataset='MCD_DBT_IV',intermdiate_target_table_name='vista_menu_item_hrchy_assn_valid') }}
  from validated
  where error_message is NULL