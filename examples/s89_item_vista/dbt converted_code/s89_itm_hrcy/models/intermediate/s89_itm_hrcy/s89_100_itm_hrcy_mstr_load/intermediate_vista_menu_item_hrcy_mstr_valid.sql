{{ config(
    tags = ['s89_itm_hrcy_grandmaster','s89_100_itm_hrcy_mstr_load', 's89_100_itm_hrcy_mstr_load_intermediate'],
    materialized='incremental',
    schema='MCD_DBT_IV'
) }}

{% set start_insert_ts = var('insert_ts', none) %}

with validated as (
  select * from {{ ref('stg_schema_compliance_check_menu_item_hrcy_mstr') }}
)

select 
    {{ cast_columns(intermdiate_target_table_project='dmgcp-del-155', intermdiate_target_table_dataset='MCD_DBT_IV',intermdiate_target_table_name='vista_menu_item_hrcy_mstr_valid') }}
from validated
  where error_message is NULL
  {% if is_incremental() %}
  and (
    {% if start_insert_ts %}
                INSERT_TS > '{{ start_insert_ts }}'
    {% else %}
    INSERT_TS > (
    select coalesce(MAX(INSERT_TS), cast('1900-01-01' as timestamp))
    from {{ source('MCD_DBT_IV', 'intermediate_vista_menu_item_hrcy_mstr_valid') }}
    )
    {% endif %}
  )
  {% endif %}