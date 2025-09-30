{{ config(
    materialized='table',
    schema='rmdw_temp',
    tags=['glma_s19_wk_mo_agg_mo_agg_grandmaster','glma_s19_wk_mo_agg_220_mo_tm_seg_sls_load'],
    pre_hook="{{ insert_mo_tm_seg_sls() }}"
) }}

select 1 as dummy_column 