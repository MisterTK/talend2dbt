# models/my_project/my_model.sql

{{ config(
    materialized='table',
     tags=['glma_s19_wk_mo_agg_wk_agg_grandmaster','glma_s19_wk_mo_agg_150_wk_tm_seg_sls_load'],
    pre_hook=[
        "{{ delete_and_insert_wkly_tm_seg() }}"
    ]
) }}

-- Dummy SQL to make the model valid
SELECT 1 AS dummy_column
