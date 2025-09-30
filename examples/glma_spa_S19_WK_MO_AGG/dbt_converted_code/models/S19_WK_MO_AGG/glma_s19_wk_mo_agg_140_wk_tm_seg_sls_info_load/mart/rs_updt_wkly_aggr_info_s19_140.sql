{{ config(
    materialized='table',
    pre_hook="{{ update_wkly_aggr_info_s19_140() }}",
    tags=['glma_s19_wk_mo_agg_wk_agg_grandmaster','glma_s19_wk_mo_agg_140_wk_tm_seg_sls_info_load']

) }}

select 1 as dummy