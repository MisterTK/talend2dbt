
{{ config(
    tags = ['s89_itm_hrcy_grandmaster','s89_100_itm_hrcy_mstr_load', 's89_100_itm_hrcy_mstr_load_staging'],
    materialized='table'
) }}

select * from {{ ref("stg_schema_compliance_check_menu_item_hrcy_mstr")}}
where error_message is not null