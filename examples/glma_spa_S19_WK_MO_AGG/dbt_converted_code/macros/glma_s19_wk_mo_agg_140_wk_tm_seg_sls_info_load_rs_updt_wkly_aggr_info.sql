{% macro update_wkly_aggr_info_s19_140() %}

UPDATE `{{ var('Redshift_gdap_Schema') }}`.WKLY_AGGR_INFO
SET 
    WKLY_TM_SEG_SLS_DY_CNT_QT = NULL,
    AGGR_TM_NON_REPT_HLDY_QT  = NULL,
    WKLY_AGGR_TM_7DY_RULE_FL  = 0,
    UPDT_DW_AUDT_TS = (
        SELECT CAST(updt_dw_audt_ts AS DATETIME)
        FROM {{ source('RMDW_RAW','JS_IN_PARMS_AND_MAX_CALDT_psv') }}
        LIMIT 1
    )
WHERE WK_END_THU_ID_NU IN (
    SELECT DISTINCT cal.WK_END_THU_ID_NU
    FROM `{{ var('Redshift_gdap_Schema') }}`.cal_dt cal
    CROSS JOIN (
        SELECT 
            DATE(wk_from_dt) AS wk_from_dt,
            DATE(wk_to_dt)   AS wk_to_dt
        FROM {{ source('RMDW_RAW','JS_IN_PARMS_AND_MAX_CALDT_psv') }}
        LIMIT 1
    ) parms
    WHERE cal.cal_dt BETWEEN parms.wk_from_dt AND parms.wk_to_dt
);

{% endmacro %}
