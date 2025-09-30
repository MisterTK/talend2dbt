{{ config(
    materialized='table',
    tags=['glma_s19_wk_mo_agg_mo_agg_grandmaster','glma_s19_wk_mo_agg_230_mo_tm_seg_sls_info_load'],
    pre_hook="{{ mo_aggr_info_tm_seg_sls_s19_230() }}"
) }}

select 1 as dummy