{{ config(
    materialized='table',
    schema='rmdw_temp',
     tags=['glma_s19_wk_mo_agg_wk_agg_grandmaster','glma_s19_wk_mo_agg_170_wk_dypt_sls_load'],
    pre_hook="{{ load_wkly_dypt_sls() }}"
) }}

select 1 as dummy_column 