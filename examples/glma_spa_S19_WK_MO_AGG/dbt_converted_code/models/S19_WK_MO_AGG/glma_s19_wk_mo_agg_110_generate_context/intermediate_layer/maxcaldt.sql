{{ config(materialized='table',
    alias='maxcaldt',
    schema='RMDW_INT',
    tags=['glma_s19_wk_mo_agg_wk_agg_grandmaster','glma_s19_wk_mo_agg_110_generate_context']) 
    }}
    
    
    WITH
  source_data AS (
    SELECT
      CAL_DT,
      YR_NU,
      MO_NU,
      MAX_SLTC_DT
    FROM
      {{ ref('hash_out_rest_rept_mo_sls') }}
  ),

  -- Calculate variables based on TalendDate functions
  variables AS (
    SELECT
      *,
      -- svcurrYear & svCurrMonth
      FORMAT_DATE('%Y', CURRENT_DATE()) AS svcurrYear,
      FORMAT_DATE('%m', CURRENT_DATE()) AS svCurrMonth,
      
      -- svMaxSltcEndOfMonthDate
      DATE_ADD(MAX_SLTC_DT, INTERVAL 32 DAY) AS svMaxSltcEndOfMonthDate
    FROM
      source_data
  ),

  -- Calculate derived variables
  derived_variables AS (
    SELECT
      *,
      -- svMaxDate
      PARSE_DATE('%Y-%m-%d', CONCAT(svcurrYear, '-', svCurrMonth, '-01')) AS svMaxDate,
      
      -- svMaxSltcYear
      CAST(FORMAT_DATE('%Y', svMaxSltcEndOfMonthDate) AS INT64) AS svMaxSltcYear,
      
      -- svMaxSltcMonth
      FORMAT_DATE('%m', svMaxSltcEndOfMonthDate) AS svMaxSltcMonth,
      
      -- svBeginMonthDate (renamed from svMonth/svYear logic for clarity)
      PARSE_DATE('%Y-%m-%d', CONCAT(CAST(YR_NU AS STRING), '-', LPAD(CAST(MO_NU AS STRING), 2, '0'), '-01')) AS svBeginMonthDate
    FROM
      variables
  ),

  -- Calculate final variables for the WHERE clause
  final_variables AS (
    SELECT
      *,
      -- var1
      PARSE_DATE('%Y-%m-%d', CONCAT(CAST(svMaxSltcYear AS STRING), '-', svMaxSltcMonth, '-01')) AS var1
    FROM
      derived_variables
  )
  
SELECT
  CAL_DT.CAL_DT AS calc_month_cal_dt,
  YR_NU AS calc_month_yr_nu,
  MO_NU AS calc_month_mo_nu,
  svBeginMonthDate AS calc_month_to_dt,
  'X' AS dummy_key
FROM
  final_variables
WHERE
  -- TalendDate.compareDate(CAL_DT, svMaxDate) == -1 translates to CAL_DT < svMaxDate
  CAL_DT.CAL_DT < svMaxDate
  -- TalendDate.compareDate(CAL_DT, svMaxSltcDate) <= 0 translates to CAL_DT <= svMaxSltcDate
  AND CAL_DT.CAL_DT <= DATE_ADD(var1, INTERVAL -1 DAY)