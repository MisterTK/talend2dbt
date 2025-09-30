{{ config(
    materialized='table',
    schema='rmdw_temp',
    tags=['glma_s19_wk_mo_agg_wk_agg_grandmaster','glma_s19_wk_mo_agg_150_wk_tm_seg_sls_load'],
    pre_hook="{{ delete_wkly_tm_seg_sls(var('Redshift_gdap_Schema')) }}"
) }}

select 1 as dummy_column 
