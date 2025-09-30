{{ config(
    materialized='table',
    alias='inparmsandmaxcaldt_join',
    schema='RMDW_INT',
    tags=['glma_s19_wk_mo_agg_wk_agg_grandmaster','glma_s19_wk_mo_agg_110_generate_context']
) }}

WITH
  -- 1. Reference all upstream models
  mo_agg_data AS (
    SELECT * FROM {{ ref('moagg_1') }}
  ),
  wk_agg_data AS (
    SELECT * FROM {{ ref('wkagg_1') }}
  ),
  max_thu_dt_data AS (
    SELECT * FROM {{ ref('hash_out_max_thu_dt') }}
  ),
  max_cal_dt_data AS (
    SELECT * FROM {{ ref('hash_out_max_cal_dt') }}
  ),

  -- 2. Join all the input tables on the DUMMY_KEY
  joined_data AS (
    SELECT
      ma.*,
      wa.* EXCEPT (dummy_key),
      mt.* EXCEPT (dummy_key),
      mc.* EXCEPT (dummy_key)
    FROM mo_agg_data AS ma
    INNER JOIN wk_agg_data AS wa
      ON ma.DUMMY_KEY = wa.DUMMY_KEY
    INNER JOIN max_thu_dt_data AS mt
      ON ma.DUMMY_KEY = mt.DUMMY_KEY
    INNER JOIN max_cal_dt_data AS mc
      ON ma.DUMMY_KEY = mc.DUMMY_KEY
  ),
  
  -- 3. Replicate Talend variables and calculations
  calculated_vars AS (
    SELECT
      *,
      -- svCalcFromDateDay
      DATE_ADD(
        PARSE_DATE('%Y-%m-%d', CONCAT(CAST(CALC_MONTH_YR_NU AS STRING), '-', LPAD(CAST(CALC_MONTH_MO_NU AS STRING), 2, '0'), '-01')),
        INTERVAL CAST(-30 * (NUM_MONTHS - 1) AS INT64) DAY
      ) AS svCalcFromDateDay
    FROM joined_data
  )
  
-- 4. Final SELECT statement with all transformations and output columns
SELECT
  -- MO_FROM_DT
  CASE
    WHEN cv.USE_NUM_MONTHS_FL = 'Y'
    THEN DATE_TRUNC(cv.svCalcFromDateDay, MONTH)
    ELSE cv.MONTH_FROM_DT
  END AS MO_FROM_DT,
  
  -- MO_TO_DT
  CASE
    WHEN cv.USE_NUM_MONTHS_FL = 'Y'
    THEN cv.CALC_MONTH_CAL_DT
    ELSE cv.MONTH_TO_DT
  END AS MO_TO_DT,
  
  -- WK_FROM_DT
  cv.WEEK_FROM_DT AS WK_FROM_DT,
  
  -- USE_NUM_WEEKS_FL
  cv.USE_NUM_WEEKS_FL,
  
  -- FROM_WK_END_THU_ID_NU
  CAST(cv.CALC_WEEK_WK_END_THU_ID_NU - cv.NUM_WEEKS + 1 AS INT64) AS FROM_WK_END_THU_ID_NU,
  
  -- WK_TO_DT
  CASE
    WHEN cv.USE_NUM_WEEKS_FL = 'Y'
    THEN cv.CALC_WEEK_CAL_DT
    ELSE cv.WEEK_TO_DT
  END AS WK_TO_DT,

  -- Straightforward renames and mappings
  cv.MONTH_RUN_PMIX AS MO_RUN_PMIX,
  cv.MONTH_RUN_TM_SEG_SLS AS MO_RUN_TM_SEG_SLS,
  cv.MONTH_RUN_DYPT_SLS AS MO_RUN_DYPT_SLS,
  cv.WEEK_RUN_PMIX AS WK_RUN_PMIX,
  cv.WEEK_RUN_TM_SEG_SLS AS WK_RUN_TM_SEG_SLS,
  cv.WEEK_RUN_DYPT_SLS AS WK_RUN_DYPT_SLS,
  cv.WEEK_RUN_SLS AS WK_RUN_SLS,
  cv.DUMMY_KEY
FROM
  calculated_vars AS cv