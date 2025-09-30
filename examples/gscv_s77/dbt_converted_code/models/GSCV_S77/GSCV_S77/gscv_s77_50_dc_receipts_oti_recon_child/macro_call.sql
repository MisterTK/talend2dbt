
{% set post_hook_statements = [] %}

{% if execute %}

    {% do post_hook_statements.append(
        reusable_js_s3_redshift_copy_withdynamicschema_new_api_new(
            target_dataset_name=var('Redshift_gdap_Stage'),
            target_table_name='oti_dc_rcpt_rjct',
            source_dataset_name='MCD_DBT',
            source_table_name='gscv_dcreceipts_fk_resolved_psv',
            is_delete=1
        )
    ) %}
{% endif %}

{{ config(
    materialized='ephemeral',
    enabled=true,
    tags = ['gscv_s77_dc_receipts_oti_recon_grandmaster','gscv_s77_50_dc_receipts_oti_recon_child'],
    post_hook=post_hook_statements
) }}


SELECT * FROM {{ ref('gscv_dcreceipts_fk_resolved_psv') }}