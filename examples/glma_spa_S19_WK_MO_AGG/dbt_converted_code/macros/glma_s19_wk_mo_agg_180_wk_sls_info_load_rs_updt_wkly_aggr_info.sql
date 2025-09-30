{% macro wkly_aggr_info_sls_reset_s19_180() %}

UPDATE `{{ var('Redshift_gdap_Schema') }}.wkly_aggr_info` T
SET
    T.WKLY_AGGR_SLS_DY_CNT_QT   = NULL,
    T.AGGR_SLS_NON_REPT_HLDY_QT = NULL,
    T.WKLY_AGGR_SLS_7DY_RULE_FL = 0,
    T.UPDT_DW_AUDT_TS            = CURRENT_DATETIME()
WHERE T.WK_END_THU_ID_NU IN (
    SELECT DISTINCT C.WK_END_THU_ID_NU
    FROM `{{ var('Redshift_gdap_Schema') }}.cal_dt` C
    CROSS JOIN (
        SELECT
            DATE(wk_from_dt) AS wk_from_dt,
            DATE(wk_to_dt)   AS wk_to_dt
        FROM {{ source('RMDW_RAW', 'JS_IN_PARMS_AND_MAX_CALDT_psv') }}
        LIMIT 1
    ) parms
    WHERE C.cal_dt BETWEEN parms.wk_from_dt AND parms.wk_to_dt
);

{% endmacro %}
