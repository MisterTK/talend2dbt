{% macro mo_aggr_info_tm_seg_sls_s19_230() %}

UPDATE `{{ var('Redshift_gdap_Schema') }}.mo_aggr_info` T
SET
    T.MO_TM_SEG_SLS_VRNC_PC    = NULL,
    T.MO_AGGR_TM_SEG_SLS_DY_QT = NULL,
    T.UPDT_DW_AUDT_TS          = CURRENT_DATETIME()
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
);

{% endmacro %}
