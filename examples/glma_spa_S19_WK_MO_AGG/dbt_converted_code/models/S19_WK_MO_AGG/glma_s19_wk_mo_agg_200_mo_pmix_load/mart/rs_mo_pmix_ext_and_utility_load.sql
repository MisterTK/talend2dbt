{{ config(
    materialized='table',
    schema='rmdw_temp',
    tags=['glma_s19_wk_mo_agg_mo_agg_grandmaster','glma_s19_wk_mo_agg_200_mo_pmix_load'],
    pre_hook="{{ load_mo_pmix() }}"
) }}

select 1 as dummy_column 