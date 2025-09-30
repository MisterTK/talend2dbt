{{ config(
    materialized='table',
    alias='js_out_parms_and_max_caldt',
    schema='rmdw_tables',
    tags=['glma_s19_wk_mo_agg_wk_agg_grandmaster','glma_s19_wk_mo_agg_110_generate_context']
) }}

WITH
  -- 1. Main input source
  in_parms_data AS (
    SELECT
      MO_FROM_DT,
      MO_TO_DT,
      WK_FROM_DT,
      USE_NUM_WEEKS_FL,
      FROM_WK_END_THU_ID_NU,
      WK_TO_DT,
      MO_RUN_PMIX,
      MO_RUN_TM_SEG_SLS,
      MO_RUN_DYPT_SLS,
      WK_RUN_PMIX,
      WK_RUN_TM_SEG_SLS,
      WK_RUN_DYPT_SLS,
      WK_RUN_SLS,
      DUMMY_KEY
    FROM {{ ref('inparmsandmaxcaldt_join') }}
  ),
  
  -- 2. Lookup table
  cal_dt_data AS (
    SELECT
      CAL_DT,
      WK_END_THU_ID_NU
    FROM {{ ref('caldt_2') }}
  ),
  
  -- 3. Join the two tables
  joined_data AS (
    SELECT
      in_parms_data.MO_FROM_DT,
      in_parms_data.MO_TO_DT,
      in_parms_data.WK_FROM_DT,
      in_parms_data.USE_NUM_WEEKS_FL,
      in_parms_data.FROM_WK_END_THU_ID_NU,
      in_parms_data.WK_TO_DT,
      in_parms_data.MO_RUN_PMIX,
      in_parms_data.MO_RUN_TM_SEG_SLS,
      in_parms_data.MO_RUN_DYPT_SLS,
      in_parms_data.WK_RUN_PMIX,
      in_parms_data.WK_RUN_TM_SEG_SLS,
      in_parms_data.WK_RUN_DYPT_SLS,
      in_parms_data.WK_RUN_SLS,
      in_parms_data.DUMMY_KEY,
      cal_dt_data.CAL_DT AS cal_dt_lookup
    FROM in_parms_data
    INNER JOIN cal_dt_data
      ON in_parms_data.FROM_WK_END_THU_ID_NU = cal_dt_data.WK_END_THU_ID_NU
  )

-- 4. Final SELECT statement with conditional logic
SELECT
  MO_FROM_DT,
  MO_TO_DT,
  -- Conditional logic for WK_FROM_DT
CASE
    WHEN joined_data.USE_NUM_WEEKS_FL = 'Y'
    THEN joined_data.cal_dt_lookup.CAL_DT
    ELSE joined_data.WK_FROM_DT
  END AS WK_FROM_DT,
  WK_TO_DT,
  MO_RUN_PMIX,
  MO_RUN_TM_SEG_SLS,
  MO_RUN_DYPT_SLS,
  WK_RUN_PMIX,
  WK_RUN_TM_SEG_SLS,
  WK_RUN_DYPT_SLS,
  WK_RUN_SLS,
  DUMMY_KEY
FROM joined_data