

{% macro load_mo_tm_seg_sls() %}

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
        {% set MO_FROM_DT = date_range.columns[0][0] %}
        {% set MO_TO_DT   = date_range.columns[1][0] %}
    {% else %}
        {% set MO_FROM_DT = '1970-01-01' %}
        {% set MO_TO_DT   = '1970-01-01' %}
    {% endif %}


    {# --- Step 2: Truncate target table --- #}
    TRUNCATE TABLE `{{ var('Redshift_gdap_Utility') }}.mo_tm_seg_sls`;


    {# --- Step 3: Insert aggregated mo_tm_seg_sls data --- #}
    {% set insert_sql %}
        INSERT INTO `{{ var('Redshift_gdap_Utility') }}.mo_tm_seg_sls`
        (
            YR_NU,
            MO_NU,
            MCD_GBAL_LCAT_ID_NU,
            TM_SEG_ID_NU,
            SALE_TYP_ID_NU,
            CURN_ISO_NU,
            MO_TM_SEG_NET_SLS_AM,
            MO_TM_SEG_TRN_CNT_QT,
            MO_TM_SEG_SLS_DY_QT
        )
        SELECT
            CAST(EXTRACT(YEAR FROM a.CAL_DT) AS INT64)  AS YR_NU,
            CAST(EXTRACT(MONTH FROM a.CAL_DT) AS INT64) AS MO_NU,
            a.MCD_GBAL_LCAT_ID_NU,
            a.TM_SEG_ID_NU,
            a.SALE_TYP_ID_NU,
            a.CURN_ISO_NU,
            CAST(SUM(a.NET_SLS_AM) AS NUMERIC)  AS MO_TM_SEG_NET_SLS_AM,
            CAST(SUM(a.TRN_CNT_QT) AS INT64)    AS MO_TM_SEG_TRN_CNT_QT,
            CAST(COUNT(*) AS INT64)             AS MO_TM_SEG_SLS_DY_QT
        FROM `{{ var('Redshift_gdap_Schema') }}.dy_tm_seg_sls` a
        WHERE a.CAL_DT BETWEEN DATE("{{ MO_FROM_DT }}") AND DATE("{{ MO_TO_DT }}")
        GROUP BY
            1,2,
            a.MCD_GBAL_LCAT_ID_NU,
            a.TM_SEG_ID_NU,
            a.SALE_TYP_ID_NU,
            a.CURN_ISO_NU
    {% endset %}

    {% do run_query(insert_sql) %}

{% endmacro %}
