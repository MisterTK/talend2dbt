{% macro mo_aggr_info_upsert_pmix_s19_210() %}

-- Step 1: Merge (Update + Insert)
MERGE `{{ var('Redshift_gdap_Schema') }}.mo_aggr_info` T
USING `{{ var('Redshift_gdap_Utility') }}.mo_aggr_info_pmix` S
ON T.YR_NU = S.YR_NU
   AND T.MO_NU = S.MO_NU
   AND T.MCD_GBAL_LCAT_ID_NU = S.MCD_GBAL_LCAT_ID_NU

WHEN MATCHED THEN
  UPDATE SET
    T.MO_PMIX_SLS_VRNC_PC      = S.MO_PMIX_SLS_VRNC_PC,
    T.MO_AGGR_PMIX_DY_QT       = S.MO_AGGR_PMIX_DY_QT,
    T.MO_AGGR_PMIX_7DY_RULE_FL = S.MO_AGGR_PMIX_7DY_RULE_FL,
    T.UPDT_DW_AUDT_TS          = CURRENT_DATETIME()

WHEN NOT MATCHED THEN
  INSERT (
    YR_NU,
    MO_NU,
    MCD_GBAL_LCAT_ID_NU,
    MO_PMIX_SLS_VRNC_PC,
    MO_AGGR_PMIX_DY_QT,
    MO_AGGR_DYPT_7DY_RULE_FL,
    MO_AGGR_PMIX_7DY_RULE_FL
  )
  VALUES (
    S.YR_NU,
    S.MO_NU,
    S.MCD_GBAL_LCAT_ID_NU,
    S.MO_PMIX_SLS_VRNC_PC,
    S.MO_AGGR_PMIX_DY_QT,
    0,
    S.MO_AGGR_PMIX_7DY_RULE_FL
  );

-- Step 2: Delete unwanted records
DELETE FROM `{{ var('Redshift_gdap_Schema') }}.mo_aggr_info` T
WHERE EXISTS (
    SELECT 1
    FROM `{{ var('Redshift_gdap_Schema') }}.cal_dt` C
    CROSS JOIN (
        SELECT
            DATE(mo_from_dt) AS mo_from_dt,
            DATE(mo_to_dt)   AS mo_to_dt
        FROM {{ source('RMDW_RAW','JS_IN_PARMS_AND_MAX_CALDT_psv') }}
        LIMIT 1
    ) parms
    WHERE C.cal_dt BETWEEN parms.mo_from_dt AND parms.mo_to_dt
      AND C.YR_NU = T.YR_NU
      AND C.MO_NU = T.MO_NU
)
AND T.MO_PMIX_SLS_VRNC_PC    IS NULL
AND T.MO_AGGR_PMIX_DY_QT     IS NULL
AND T.MO_DYPT_SLS_VRNC_PC    IS NULL
AND T.MO_AGGR_DYPT_SLS_DY_QT IS NULL;

{% endmacro %}
