{{ config(
    materialized='table',
    tags=['glma_s19_wk_mo_agg_mo_agg_grandmaster','glma_s19_wk_mo_agg_250_mo_dypt_sls_info_load'],
    pre_hook="{{ update_mo_aggr_info_s19_250() }}"
) }}

select 1 as dummy