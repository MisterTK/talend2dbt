{% macro update_mo_aggr_info_pm() %}

UPDATE `{{ var('Redshift_gdap_Schema') }}.mo_aggr_info` AS target
SET
    MO_PMIX_SLS_VRNC_PC = NULL,
    MO_AGGR_PMIX_DY_QT  = NULL,
    MO_AGGR_PMIX_7DY_RULE_FL = 0,
    UPDT_DW_AUDT_TS = (
        SELECT CAST(CURRENT_TIMESTAMP() AS DATETIME)
        FROM {{ source('RMDW_RAW','JS_IN_PARMS_AND_MAX_CALDT_psv') }}
        LIMIT 1
    )
WHERE (YR_NU * 100 + MO_NU) IN (
    SELECT DISTINCT (cal.YR_NU * 100 + cal.MO_NU)
    FROM `{{ var('Redshift_gdap_Schema') }}.cal_dt` AS cal
    CROSS JOIN (
        SELECT
            DATE(mo_from_dt) AS mo_from_dt,
            DATE(mo_to_dt)   AS mo_to_dt
        FROM {{ source('RMDW_RAW','JS_IN_PARMS_AND_MAX_CALDT_psv') }}
        LIMIT 1
    ) AS parms
    WHERE cal.cal_dt BETWEEN parms.mo_from_dt AND parms.mo_to_dt
);

{% endmacro %}
