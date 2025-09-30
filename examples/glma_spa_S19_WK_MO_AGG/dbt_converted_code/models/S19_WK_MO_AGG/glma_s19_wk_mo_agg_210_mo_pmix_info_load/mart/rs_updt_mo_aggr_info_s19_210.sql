{{ config(
    materialized='table',
    tags=['glma_s19_wk_mo_agg_mo_agg_grandmaster','glma_s19_wk_mo_agg_210_mo_pmix_info_load'],
    pre_hook="{{ update_mo_aggr_info_pm() }}"
) }}

select 1 as dummy