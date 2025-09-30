{% macro load_mo_pmix() %}

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

 
        TRUNCATE TABLE  `{{ var('Redshift_gdap_Utility') }}.mo_pmix`
        
   

    {# --- Step 3: Insert aggregated mo_pmix data --- #}
    {% set insert_sql %}
        INSERT INTO `{{ var('Redshift_gdap_Utility') }}.mo_pmix`
        (
            YR_NU,
            MO_NU,
            MO_PERD_NU,
            MCD_GBAL_LCAT_ID_NU,
            TERR_CD,
            SLD_MENU_ITM_ID,
            PMIX_UNT_TYP_ID_NU,
            CURN_ISO_NU,
            MO_PMIX_NET_PRC_AM,
            MO_CMBO_UP_DWN_AM,
            MO_POS_KEY_NET_PRC_AM,
            FOOD_COST_AM,
            PAPR_COST_AM,
            OTH_COST_AM,
            TOT_COST_AM,
            MO_POS_KEY_QT,
            MO_CMBO_UNTS_SLD_QT,
            MO_TOT_SLD_QT,
            MO_PMIX_DY_CNT_QT,
            MO_ITM_CMBO_UP_DWN_AM
        )
        SELECT
            EXTRACT(YEAR FROM a.cal_dt) AS YR_NU,
            EXTRACT(MONTH FROM a.cal_dt) AS MO_NU,
            MO_PERD_NU,
            a.MCD_GBAL_LCAT_ID_NU,
            a.TERR_CD,
            a.SLD_MENU_ITM_ID,
            a.PMIX_UNT_TYP_ID_NU,
            a.CURN_ISO_NU,
            a.DLY_PMIX_NET_PRC_AM AS MO_PMIX_NET_PRC_AM,
            a.CMBO_UP_DWN_AM AS MO_CMBO_UP_DWN_AM,
            a.POS_KEY_NET_PRC_AM AS MO_POS_KEY_NET_PRC_AM,
            a2.FOOD_COST_AM,
            a2.PAPR_COST_AM,
            a2.OTH_COST_AM,
            a2.TOT_COST_AM,
            SUM(a.POS_KEY_QT) AS MO_POS_KEY_QT,
            SUM(a.CMBO_UNTS_SLD_QT) AS MO_CMBO_UNTS_SLD_QT,
            SUM(a.TOT_SLD_QT) AS MO_TOT_SLD_QT,
            COUNT(*) AS MO_PMIX_DY_CNT_QT,
            a.ITM_CMBO_UP_DWN_AM AS MO_ITM_CMBO_UP_DWN_AM
        FROM `{{ var('Redshift_gdap_Schema') }}.dly_pmix` a
        INNER JOIN (
            SELECT
                EXTRACT(YEAR FROM cal_dt) AS YR_NU,
                EXTRACT(MONTH FROM cal_dt) AS MO_NU,
                MCD_GBAL_LCAT_ID_NU,
                TERR_CD,
                SLD_MENU_ITM_ID,
                PMIX_UNT_TYP_ID_NU,
                DLY_PMIX_NET_PRC_AM,
                COALESCE(CMBO_UP_DWN_AM,0) AS CMBO_UP_DWN_AM,
                COALESCE(POS_KEY_NET_PRC_AM,0) AS POS_KEY_NET_PRC_AM,
                MAX(cal_dt) AS MAX_CAL_DT,
                COALESCE(ITM_CMBO_UP_DWN_AM,0) AS ITM_CMBO_UP_DWN_AM
            FROM `{{ var('Redshift_gdap_Schema') }}.dly_pmix`
            WHERE cal_dt BETWEEN DATE("{{ MO_FROM_DT }}") AND DATE("{{ MO_TO_DT }}")
            GROUP BY
                YR_NU, MO_NU, MCD_GBAL_LCAT_ID_NU, TERR_CD, SLD_MENU_ITM_ID,
                PMIX_UNT_TYP_ID_NU, DLY_PMIX_NET_PRC_AM,
                COALESCE(CMBO_UP_DWN_AM,0), COALESCE(POS_KEY_NET_PRC_AM,0),
                COALESCE(ITM_CMBO_UP_DWN_AM,0)
        ) a1
            ON a1.YR_NU = EXTRACT(YEAR FROM a.cal_dt)
           AND a1.MO_NU = EXTRACT(MONTH FROM a.cal_dt)
           AND a1.MCD_GBAL_LCAT_ID_NU = a.MCD_GBAL_LCAT_ID_NU
           AND a1.TERR_CD = a.TERR_CD
           AND a1.SLD_MENU_ITM_ID = a.SLD_MENU_ITM_ID
           AND a1.PMIX_UNT_TYP_ID_NU = a.PMIX_UNT_TYP_ID_NU
           AND a1.DLY_PMIX_NET_PRC_AM = a.DLY_PMIX_NET_PRC_AM
           AND COALESCE(a1.CMBO_UP_DWN_AM,0) = COALESCE(a.CMBO_UP_DWN_AM,0)
           AND COALESCE(a1.POS_KEY_NET_PRC_AM,0) = COALESCE(a.POS_KEY_NET_PRC_AM,0)
           AND COALESCE(a1.ITM_CMBO_UP_DWN_AM,0) = COALESCE(a.ITM_CMBO_UP_DWN_AM,0)
           AND a.TERR_CD = 840
        INNER JOIN `{{ var('Redshift_gdap_Schema') }}.dly_pmix` a2
            ON a2.cal_dt = a1.MAX_CAL_DT
           AND a2.MCD_GBAL_LCAT_ID_NU = a1.MCD_GBAL_LCAT_ID_NU
           AND a2.TERR_CD = a1.TERR_CD
           AND a2.SLD_MENU_ITM_ID = a1.SLD_MENU_ITM_ID
           AND a2.PMIX_UNT_TYP_ID_NU = a1.PMIX_UNT_TYP_ID_NU
           AND a2.DLY_PMIX_NET_PRC_AM = a1.DLY_PMIX_NET_PRC_AM
           AND COALESCE(a2.CMBO_UP_DWN_AM,0) = COALESCE(a1.CMBO_UP_DWN_AM,0)
           AND COALESCE(a2.POS_KEY_NET_PRC_AM,0) = COALESCE(a1.POS_KEY_NET_PRC_AM,0)
           AND COALESCE(a2.ITM_CMBO_UP_DWN_AM,0) = COALESCE(a1.ITM_CMBO_UP_DWN_AM,0)
           AND a2.TERR_CD = 840
           AND a2.cal_dt BETWEEN DATE("{{ MO_FROM_DT }}") AND DATE("{{ MO_TO_DT }}")
        INNER JOIN `{{ var('Redshift_gdap_Schema') }}.mo` c
            ON c.YR_NU = EXTRACT(YEAR FROM a.cal_dt)
           AND c.MO_NU = EXTRACT(MONTH FROM a.cal_dt)
        WHERE a.cal_dt BETWEEN DATE("{{ MO_FROM_DT }}") AND DATE("{{ MO_TO_DT }}")
        GROUP BY
            1,2,MO_PERD_NU,a.MCD_GBAL_LCAT_ID_NU,a.TERR_CD,a.SLD_MENU_ITM_ID,
            a.PMIX_UNT_TYP_ID_NU,a.CURN_ISO_NU,a.DLY_PMIX_NET_PRC_AM,
            a.CMBO_UP_DWN_AM,a.POS_KEY_NET_PRC_AM,a2.FOOD_COST_AM,a2.PAPR_COST_AM,
            a2.OTH_COST_AM,a2.TOT_COST_AM,a.ITM_CMBO_UP_DWN_AM
    {% endset %}

    {% do run_query(insert_sql) %}

{% endmacro %}
