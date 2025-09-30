{% macro update_part1() %}

update {{ var('Redshift_gdap_Schema') }}.WKLY_AGGR_INFO
set
    WKLY_AGGR_PMIX_DY_CNT_QT = null,
    AGGR_PMIX_NON_REPT_HLDY_QT = null,
    WKLY_AGGR_PMIX_7DY_RULE_FL = 0,
    UPDT_DW_AUDT_TS = (
        select cast(current_timestamp() as DATETIME)
        from {{ source('RMDW_RAW','JS_IN_PARMS_AND_MAX_CALDT_psv') }}
        limit 1
    )
where WK_END_THU_ID_NU in (
    select distinct WK_END_THU_ID_NU
    from {{ var('Redshift_gdap_Schema') }}.cal_dt
    cross join (
        select
            date(wk_from_dt) as wk_from_dt,
            date(wk_to_dt) as wk_to_dt
        from {{ source('RMDW_RAW','JS_IN_PARMS_AND_MAX_CALDT_psv') }}
        limit 1
    ) parms
    where cal_dt.cal_dt between parms.wk_from_dt and parms.wk_to_dt

)

{% endmacro %}
