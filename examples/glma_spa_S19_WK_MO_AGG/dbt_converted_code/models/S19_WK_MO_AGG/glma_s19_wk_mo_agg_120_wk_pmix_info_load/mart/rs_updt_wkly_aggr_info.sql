{{ config(
    materialized='table',
    pre_hook="{{ update_part1() }}",
    tags=['glma_s19_wk_mo_agg_wk_agg_grandmaster','glma_s19_wk_mo_agg_120_wk_pmix_info_load']
) }}



select 1 as dummy
