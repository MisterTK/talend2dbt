{% macro merge_wkly_aggr_info_s19_140() %}

MERGE {{ var('Redshift_gdap_Schema') }}.WKLY_AGGR_INFO AS T
USING (
    SELECT
        WK_END_THU_ID_NU,
        MCD_GBAL_LCAT_ID_NU,
        WKLY_TM_SEG_SLS_DY_CNT_QT,
        AGGR_TM_NON_REPT_HLDY_QT,
        WKLY_AGGR_TM_7DY_RULE_FL
    FROM {{ var('Redshift_gdap_Utility') }}.WKLY_AGGR_INFO_TM_SEG_SLS
) AS S
ON T.WK_END_THU_ID_NU = S.WK_END_THU_ID_NU
   AND T.MCD_GBAL_LCAT_ID_NU = S.MCD_GBAL_LCAT_ID_NU

-- Update if row exists
WHEN MATCHED THEN
  UPDATE SET
    WKLY_TM_SEG_SLS_DY_CNT_QT = S.WKLY_TM_SEG_SLS_DY_CNT_QT,
    AGGR_TM_NON_REPT_HLDY_QT  = S.AGGR_TM_NON_REPT_HLDY_QT,
    WKLY_AGGR_TM_7DY_RULE_FL  = cast(S.WKLY_AGGR_TM_7DY_RULE_FL as int64),
    UPDT_DW_AUDT_TS           = CURRENT_DATETIME()

-- Insert if row not present
WHEN NOT MATCHED THEN
  INSERT (
      WK_END_THU_ID_NU,
      MCD_GBAL_LCAT_ID_NU,
      WKLY_TM_SEG_SLS_DY_CNT_QT,
      AGGR_TM_NON_REPT_HLDY_QT,
      WKLY_AGGR_PMIX_7DY_RULE_FL,
      WKLY_AGGR_SLS_7DY_RULE_FL,
      WKLY_AGGR_TM_7DY_RULE_FL
  )
  VALUES (
      S.WK_END_THU_ID_NU,
      S.MCD_GBAL_LCAT_ID_NU,
      S.WKLY_TM_SEG_SLS_DY_CNT_QT,
      S.AGGR_TM_NON_REPT_HLDY_QT,
      0,
      0,
      cast(cast(S.WKLY_AGGR_TM_7DY_RULE_FL as int64) as int64)
  );

-- Delete orphan rows (must be a separate statement in BigQuery)
DELETE FROM {{ var('Redshift_gdap_Schema') }}.WKLY_AGGR_INFO
WHERE WK_END_THU_ID_NU IN (
    SELECT DISTINCT cal_dt.WK_END_THU_ID_NU
    FROM `{{ var('Redshift_gdap_Schema') }}`.cal_dt cal_dt
    CROSS JOIN (
        SELECT
            DATE(wk_from_dt) AS wk_from_dt,
            DATE(wk_to_dt)   AS wk_to_dt
        FROM {{ source('RMDW_RAW','JS_IN_PARMS_AND_MAX_CALDT_psv') }}
        LIMIT 1
    ) parms
    WHERE cal_dt.cal_dt BETWEEN parms.wk_from_dt AND parms.wk_to_dt
)
AND   WKLY_AGGR_PMIX_DY_CNT_QT   IS NULL
AND   WKLY_AGGR_SLS_DY_CNT_QT    IS NULL
AND   WKLY_TM_SEG_SLS_DY_CNT_QT  IS NULL
AND   AGGR_PMIX_NON_REPT_HLDY_QT IS NULL
AND   AGGR_SLS_NON_REPT_HLDY_QT  IS NULL
AND   AGGR_TM_NON_REPT_HLDY_QT   IS NULL;

{% endmacro %}
