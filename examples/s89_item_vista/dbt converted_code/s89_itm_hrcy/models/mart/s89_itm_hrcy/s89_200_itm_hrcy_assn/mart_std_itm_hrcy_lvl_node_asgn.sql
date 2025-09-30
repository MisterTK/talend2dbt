{{ config(
    materialized='table',
    tags = ['s89_itm_hrcy_grandmaster','s89_200_itm_hrcy_assn','s89_200_itm_hrcy_assn_mart']
) }}

{% if execute %}
  {% do run_query(redshift_delete_and_insert_2(
      target_relation='rmdw_tables.std_itm_hrcy_lvl_node_asgn',
      source_relation=ref('intermediate_std_itm_hrcy_lvl_node_asgn_stg'),
      join_key_columns=['itm_hrcy_node_id']
  )) %}
{% endif %}

SELECT * FROM {{ ref('intermediate_std_itm_hrcy_lvl_node_asgn_stg') }}