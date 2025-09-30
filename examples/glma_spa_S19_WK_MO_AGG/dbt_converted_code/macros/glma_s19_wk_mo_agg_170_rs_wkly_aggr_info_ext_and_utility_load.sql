{% macro load_wkly_dypt_sls() %}

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
            FORMAT_DATE('%Y-%m-%d', WK_TO_DT) AS WK_TO_DT
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

    {{ log("Deleting all rows from " ~ var('Redshift_gdap_Utility') ~ ".wkly_dypt_sls_wk_end_thu", info=True) }}

    TRUNCATE TABLE {{ var('Redshift_gdap_Utility') }}.wkly_dypt_sls_wk_end_thu;

    {{ log("Inserting rows for date range " ~ wk_from_dt ~ " to " ~ wk_to_dt, info=True) }}

    INSERT INTO {{ var('Redshift_gdap_Utility') }}.wkly_dypt_sls_wk_end_thu (
        wk_end_thu_id_nu,
        wk_end_thu_end_dt,
        mcd_gbal_lcat_id_nu,
        sale_typ_id_nu,
        curn_iso_nu,
        dypt_id_nu,
        wkly_dypt_net_sls_am,
        wkly_dypt_trn_cnt_qt,
        dypt_sls_dy_cnt_qt,
        wkly_dypt_non_rept_hldy_qt,
        wkly_aggr_dypt_7dy_rule_fl
    )
    SELECT
        CAST(DIV(DATE_DIFF(a.cal_dt, DATE '1970-01-02', DAY), 7) + 2 AS INT64) AS wk_end_thu_id_nu,
        t.wk_end_thu_end_dt,
        a.mcd_gbal_lcat_id_nu,
        a.sale_typ_id_nu,
        a.curn_iso_nu,
        a2.dypt_id_nu,
        CAST(SUM(a.net_sls_am) AS NUMERIC) AS wkly_dypt_net_sls_am,
        CAST(SUM(COALESCE(a.trn_cnt_qt, 0)) AS INT64) AS wkly_dypt_trn_cnt_qt,
        CAST(COUNT(DISTINCT a.cal_dt) AS INT64) AS dypt_sls_dy_cnt_qt,
        CAST(
            COALESCE(c.wk_end_thu_hldy_dy_cnt_qt, 0) - COUNT(DISTINCT b.cal_dt)
            AS INT64
        ) AS wkly_dypt_non_rept_hldy_qt,
        i.wkly_aggr_dypt_7dy_rule_fl
    FROM {{ var('Redshift_gdap_Schema') }}.dy_tm_seg_sls a
    INNER JOIN {{ var('Redshift_gdap_Schema') }}.mcd_gbal_busn_lcat a1
        ON a1.mcd_gbal_lcat_id_nu = a.mcd_gbal_lcat_id_nu
    INNER JOIN {{ var('Redshift_gdap_Schema') }}.dy_tm_seg a2
        ON a2.tm_seg_id_nu = a.tm_seg_id_nu
       AND a2.ctry_iso_nu = a1.ctry_iso_nu
    LEFT JOIN {{ var('Redshift_gdap_Schema') }}.ctry_hldy b
        ON b.ctry_iso_nu = a1.ctry_iso_nu
       AND b.cal_dt = a.cal_dt
       AND b.xpct_clsd_dy_fl = 1
    LEFT JOIN (
        SELECT
            a.wk_end_thu_id_nu,
            b.ctry_iso_nu,
            COUNT(*) AS wk_end_thu_hldy_dy_cnt_qt
        FROM {{ var('Redshift_gdap_Schema') }}.cal_dt a
        INNER JOIN {{ var('Redshift_gdap_Schema') }}.ctry_hldy b
            ON b.cal_dt = a.cal_dt
        WHERE b.xpct_clsd_dy_fl = 1
        GROUP BY a.wk_end_thu_id_nu, b.ctry_iso_nu
    ) c
        ON c.wk_end_thu_id_nu = CAST(DIV(DATE_DIFF(a.cal_dt, DATE '1970-01-02', DAY), 7) + 2 AS INT64)
       AND c.ctry_iso_nu = a1.ctry_iso_nu
    INNER JOIN {{ var('Redshift_gdap_Schema') }}.wk_end_thu t
        ON t.wk_end_thu_id_nu = CAST(DIV(DATE_DIFF(a.cal_dt, DATE '1970-01-02', DAY), 7) + 2 AS INT64)
    INNER JOIN {{ var('Redshift_gdap_Schema') }}.wkly_aggr_info i
        ON i.wk_end_thu_id_nu = CAST(DIV(DATE_DIFF(a.cal_dt, DATE '1970-01-02', DAY), 7) + 2 AS INT64)
       AND i.mcd_gbal_lcat_id_nu = a.mcd_gbal_lcat_id_nu
    WHERE a.cal_dt BETWEEN DATE('{{ wk_from_dt }}') AND DATE('{{ wk_to_dt }}')
      AND a1.ctry_iso_nu IN (840)
    GROUP BY
        wk_end_thu_id_nu,
        t.wk_end_thu_end_dt,
        a.mcd_gbal_lcat_id_nu,
        a.sale_typ_id_nu,
        a.curn_iso_nu,
        a2.dypt_id_nu,
        c.wk_end_thu_hldy_dy_cnt_qt,
        i.wkly_aggr_dypt_7dy_rule_fl;

{% endmacro %}
