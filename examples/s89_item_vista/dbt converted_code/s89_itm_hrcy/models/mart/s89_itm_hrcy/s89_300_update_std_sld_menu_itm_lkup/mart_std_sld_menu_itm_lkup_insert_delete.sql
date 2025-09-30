{% set post_hook_statements = [] %}

{% if execute %}

    {% do post_hook_statements.append(
        conditional_smi_lkup_load_and_upsert(
            target_relation=this,
            source_model_ref=ref('intermediate_std_sld_menu_itm_lkup_base'),
            stage_smi_source_ref=source('Redshift_gdap_Stage', 'std_sld_menu_itm')
        )
    ) %}
{% endif %}

{{ config(
    materialized='ephemeral',
    tags = ['s89_itm_hrcy_grandmaster','s89_300_update_std_sld_menu_itm_lkup','s89_300_update_std_sld_menu_itm_lkup_mart'],
    post_hook=post_hook_statements
) }}


SELECT * FROM {{ ref('intermediate_std_sld_menu_itm_lkup_base') }}