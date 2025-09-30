{{ config(
    materialized='table',
    tags=['s89_itm_hrcy_grandmaster', 's89_200_itm_hrcy_assn', 's89_200_itm_hrcy_assn_intermediate']
) }}

{% if execute %}
  {% do run_query(REUSABLE_JS_S3_REDSHIFT_copy_withdynamicschema_new_API_updated(
      target_dataset_name='rmdw_stge',
      target_table_name='std_itm_hrcy_node_asgn',
      source_dataset_name='MCD_DBT_IV',
      source_table_name='intermediate_std_itm_hrcy_node_asgn',
      is_delete=1
  )) %}
{% endif %}

SELECT * FROM {{ ref('intermediate_std_itm_hrcy_node_asgn') }}