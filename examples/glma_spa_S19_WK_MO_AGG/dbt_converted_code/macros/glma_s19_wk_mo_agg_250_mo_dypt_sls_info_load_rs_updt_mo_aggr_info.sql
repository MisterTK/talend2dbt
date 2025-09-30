{% macro update_mo_aggr_info_s19_250() %}



    {% set date_query %}
        WITH date_context AS (
            SELECT
                MO_FROM_DT,
                MO_TO_DT
            FROM {{ source('RMDW_RAW', 'JS_IN_PARMS_AND_MAX_CALDT_psv') }}
            QUALIFY ROW_NUMBER() OVER (ORDER BY MO_FROM_DT DESC) = 1
        )
        SELECT
            FORMAT_DATE('%Y-%m-%d', MO_FROM_DT) AS MO_FROM_DT,
            FORMAT_DATE('%Y-%m-%d', MO_TO_DT)   AS MO_TO_DT
        FROM date_context
    {% endset %}

    {% if execute %}
        {% set date_range = run_query(date_query) %}
        {% set MO_FROM_DT = date_range.columns[0].values()[0] %}
        {% set MO_TO_DT   = date_range.columns[1].values()[0] %}
    {% else %}
        {% set MO_FROM_DT = '1970-01-01' %}
        {% set MO_TO_DT   = '1970-01-01' %}
    {% endif %}

    {% set update_sql %}
        UPDATE `{{ var('Redshift_gdap_Schema') }}.mo_aggr_info` AS target
        SET 
            MO_DYPT_SLS_VRNC_PC      = NULL,
            MO_AGGR_DYPT_SLS_DY_QT   = NULL,
            MO_AGGR_DYPT_7DY_RULE_FL = 0,
            UPDT_DW_AUDT_TS = CURRENT_DATETIME()
        WHERE (target.YR_NU * 100 + target.MO_NU) IN (
            SELECT DISTINCT YR_NU * 100 + MO_NU
            FROM `{{ var('Redshift_gdap_Schema') }}.cal_dt`
            WHERE cal_dt BETWEEN DATE('{{ MO_FROM_DT }}') AND DATE('{{ MO_TO_DT }}')
        )
    {% endset %}

    {% do run_query(update_sql) %}

{% endmacro %}
