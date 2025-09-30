{{ config(
    materialized='table',
    tags=['glma_s19_wk_mo_agg_wk_agg_grandmaster','glma_s19_wk_mo_agg_180_wk_sls_info_load'],
    pre_hook="{{ wkly_aggr_info_sls_reset_s19_180() }}"
) }}

select 1 as dummy