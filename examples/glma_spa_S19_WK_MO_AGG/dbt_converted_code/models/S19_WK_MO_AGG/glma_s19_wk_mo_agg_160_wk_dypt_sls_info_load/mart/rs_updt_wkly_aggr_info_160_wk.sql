
{{ config(
    materialized='table',
    tags=['glma_s19_wk_mo_agg_wk_agg_grandmaster','glma_s19_wk_mo_agg_160_wk_dypt_sls_info_load'],
    pre_hook="{{ glma_s19_wk_mo_agg_160_update_wkly_aggr_info() }}"
) }}



select 1 as dummy