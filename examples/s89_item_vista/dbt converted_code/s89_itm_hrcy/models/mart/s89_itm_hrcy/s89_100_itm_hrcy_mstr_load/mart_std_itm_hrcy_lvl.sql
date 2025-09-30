{{ config(
    materialized='ephemeral',
    tags=['s89_itm_hrcy_grandmaster', 's89_100_itm_hrcy_mstr_load', 's89_100_itm_hrcy_mstr_load_mart']
) }}

{% if execute %}
    {% do run_query(REUSABLE_JS_S3_REDSHIFT_copy_withdynamicschema_new_API_updated(
        target_dataset_name='rmdw_tables',
        target_table_name='std_itm_hrcy_lvl',
        source_dataset_name='MCD_DBT_IV',
        source_table_name='std_itm_hrcy_lvl',
        is_delete=1
    )) %}
{% endif %}

SELECT * FROM {{ ref('intermediate_std_itm_hrcy_lvl') }}