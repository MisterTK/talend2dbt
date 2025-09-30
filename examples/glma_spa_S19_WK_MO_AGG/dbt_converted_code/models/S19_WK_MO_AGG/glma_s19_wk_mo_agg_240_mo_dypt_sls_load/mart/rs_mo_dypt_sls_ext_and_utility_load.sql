{{ config(
    materialized='table',
    schema='rmdw_temp',
    tags=['glma_s19_wk_mo_agg_mo_agg_grandmaster','glma_s19_wk_mo_agg_240_mo_dypt_sls_load'],
    pre_hook="{{  mo_dypt_sls_insert() }}"
) }}

select 1 as dummy_column 