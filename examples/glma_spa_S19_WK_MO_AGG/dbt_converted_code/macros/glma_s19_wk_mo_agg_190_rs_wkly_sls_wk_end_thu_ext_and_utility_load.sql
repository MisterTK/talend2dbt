
{% macro load_wkly_sls_wk_end_thu() %}

  
    {% set date_query %}
        WITH date_context AS (
            SELECT
                WK_FROM_DT,
                WK_TO_DT
            FROM {{ source('RMDW_RAW', 'JS_IN_PARMS_AND_MAX_CALDT_psv') }}
            QUALIFY ROW_NUMBER() OVER (ORDER BY WK_FROM_DT DESC) = 1
        )
        SELECT
            FORMAT_DATE('%Y-%m-%d', WK_FROM_DT) AS WK_FROM_DT,
            FORMAT_DATE('%Y-%m-%d', WK_TO_DT)   AS WK_TO_DT
        FROM date_context
    {% endset %}

    {% set date_range = run_query(date_query) %}
    {% if execute %}
        {% set wk_from_dt = date_range.columns[0].values()[0] %}
        {% set wk_to_dt   = date_range.columns[1].values()[0] %}
    {% else %}
        {% set wk_from_dt = '1970-01-01' %}
        {% set wk_to_dt   = '1970-01-01' %}
    {% endif %}

 
    
        TRUNCATE TABLE  `{{ var('Redshift_gdap_Utility') }}.wkly_sls_wk_end_thu`
        
 

   
    {% set insert_sql %}
        INSERT INTO `{{ var('Redshift_gdap_Utility') }}.wkly_sls_wk_end_thu`
        (
            WK_END_THU_ID_NU,
            WK_END_THU_END_DT,
            MCD_GBAL_LCAT_ID_NU,
            SALE_TYP_ID_NU,
            CURN_ISO_NU,
            WKLY_NET_SLS_AM,
            WKLY_TRN_CNT_QT,
            WKLY_SLD_REDM_QT,
            WKLY_SLS_DY_CNT_QT,
            WKLY_NON_REPT_HLDY_QT,
            WKLY_AGGR_SLS_7DY_RULE_FL
        )
        SELECT
            CAST((DATE_DIFF(a.cal_dt, DATE '1970-01-02', DAY) / 7 + 2) AS INT64) AS WK_END_THU_ID_NU,
            t.WK_END_THU_END_DT,
            a.MCD_GBAL_LCAT_ID_NU,
            a.SALE_TYP_ID_NU,
            a.CURN_ISO_NU,
            CAST(SUM(a.DLY_NET_SLS_AM) AS NUMERIC) AS WKLY_NET_SLS_AM,
            SUM(a.DLY_TRN_CNT_QT) AS WKLY_TRN_CNT_QT,
            SUM(a.DLY_SLD_REDM_QT) AS WKLY_SLD_REDM_QT,
            CAST(COUNT(*) AS INT64) AS WKLY_SLS_DY_CNT_QT,
            CAST(
                COALESCE(c.WK_END_THU_HLDY_DY_CNT_QT, 0) -
                SUM(CASE WHEN b.cal_dt IS NULL THEN 0 ELSE 1 END)
                AS INT64
            ) AS WKLY_NON_REPT_HLDY_QT,
            i.WKLY_AGGR_SLS_7DY_RULE_FL
        FROM `{{ var('Redshift_gdap_Schema') }}.dly_sls` a
        INNER JOIN `{{ var('Redshift_gdap_Schema') }}.mcd_gbal_busn_lcat` a1
            ON a1.MCD_GBAL_LCAT_ID_NU = a.MCD_GBAL_LCAT_ID_NU
        LEFT JOIN `{{ var('Redshift_gdap_Schema') }}.ctry_hldy` b
            ON b.CTRY_ISO_NU = a1.CTRY_ISO_NU
            AND b.cal_dt = a.cal_dt
            AND b.XPCT_CLSD_DY_FL = 1
        LEFT JOIN (
            SELECT
                a.WK_END_THU_ID_NU,
                b.CTRY_ISO_NU,
                COUNT(*) AS WK_END_THU_HLDY_DY_CNT_QT
            FROM `{{ var('Redshift_gdap_Schema') }}.cal_dt` a
            INNER JOIN `{{ var('Redshift_gdap_Schema') }}.ctry_hldy` b
                ON b.cal_dt = a.cal_dt
            WHERE b.XPCT_CLSD_DY_FL = 1
            GROUP BY a.WK_END_THU_ID_NU, b.CTRY_ISO_NU
        ) c
            ON c.WK_END_THU_ID_NU = CAST((DATE_DIFF(a.cal_dt, DATE '1970-01-02', DAY) / 7 + 2) AS INT64)
            AND c.CTRY_ISO_NU = a1.CTRY_ISO_NU
        INNER JOIN `{{ var('Redshift_gdap_Schema') }}.wk_end_thu` t
            ON t.WK_END_THU_ID_NU = CAST((DATE_DIFF(a.cal_dt, DATE '1970-01-02', DAY) / 7 + 2) AS INT64)
        INNER JOIN `{{ var('Redshift_gdap_Schema') }}.WKLY_AGGR_INFO` i
            ON i.WK_END_THU_ID_NU = CAST((DATE_DIFF(a.cal_dt, DATE '1970-01-02', DAY) / 7 + 2) AS INT64)
            AND i.MCD_GBAL_LCAT_ID_NU = a.MCD_GBAL_LCAT_ID_NU
        WHERE a.cal_dt BETWEEN DATE('{{ wk_from_dt }}') AND DATE('{{ wk_to_dt }}')
          AND a1.CTRY_ISO_NU IN (840)
        GROUP BY
            1, t.WK_END_THU_END_DT, a.MCD_GBAL_LCAT_ID_NU, a.SALE_TYP_ID_NU, a.CURN_ISO_NU,
            c.WK_END_THU_HLDY_DY_CNT_QT, i.WKLY_AGGR_SLS_7DY_RULE_FL
    {% endset %}
    {% do run_query(insert_sql) %}

{% endmacro %}
