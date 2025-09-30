  
{{ config(
    materialized='table',
    alias='hash_out_max_cal_dt',
    schema='RMDW_INT',
    tags=['glma_s19_wk_mo_agg_wk_agg_grandmaster','glma_s19_wk_mo_agg_110_generate_context']
) }}

WITH sorted_data AS (
    SELECT
        calc_month_cal_dt,
        calc_month_yr_nu,
        calc_month_mo_nu,
        calc_month_to_dt,
        dummy_key,
        ROW_NUMBER() OVER (
            PARTITION BY dummy_key
            ORDER BY
                dummy_key ASC,
                calc_month_cal_dt DESC
        ) AS row_num
    FROM
        {{ ref('maxcaldt') }}
)

SELECT
    calc_month_cal_dt,
    calc_month_yr_nu,
    calc_month_mo_nu,
    calc_month_to_dt,
    dummy_key
FROM
    sorted_data
WHERE
    row_num = 1