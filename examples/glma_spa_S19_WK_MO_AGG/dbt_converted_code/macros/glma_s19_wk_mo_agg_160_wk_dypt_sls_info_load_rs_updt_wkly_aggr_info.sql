{% macro glma_s19_wk_mo_agg_160_update_wkly_aggr_info() %}

update {{ var('Redshift_gdap_Schema') }}.WKLY_AGGR_INFO
set
    WKLY_DYPT_SLS_DY_CNT_QT   = null,
    AGGR_DYPT_NON_REPT_HLDY_QT = null,
    WKLY_AGGR_DYPT_7DY_RULE_FL = 0,
    UPDT_DW_AUDT_TS = (
        select cast(current_timestamp() as DATETIME)
        from {{ source('RMDW_RAW','JS_IN_PARMS_AND_MAX_CALDT_psv') }}
        limit 1
    )
where WK_END_THU_ID_NU in (
    select distinct cal_dt.WK_END_THU_ID_NU
    from {{ var('Redshift_gdap_Schema') }}.cal_dt cal_dt
    cross join (
        select
            date(wk_from_dt) as wk_from_dt,
            date(wk_to_dt)   as wk_to_dt
        from {{ source('RMDW_RAW','JS_IN_PARMS_AND_MAX_CALDT_psv') }}
        limit 1
    ) parms
    where cal_dt.cal_dt between parms.wk_from_dt and parms.wk_to_dt
)

{% endmacro %}
