{% macro delete_mo_pmix() %}

    {# --- Step 1: Get date range from source table --- #}
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

    {# --- Step 2: Delete data from mo_pmix --- #}
    {% set delete_sql %}
        DELETE FROM `{{ var('Redshift_gdap_Schema') }}.mo_pmix` AS mp
        WHERE EXISTS (
            SELECT 1
            FROM `{{ var('Redshift_gdap_Schema') }}.cal_dt` AS c
            WHERE c.cal_dt BETWEEN DATE("{{ MO_FROM_DT }}") AND DATE("{{ MO_TO_DT }}")
              AND mp.YR_NU = c.YR_NU
              AND mp.MO_NU = c.MO_NU
        )
        AND mp.TERR_CD = 840
    {% endset %}

    {% do run_query(delete_sql) %}

{% endmacro %}
