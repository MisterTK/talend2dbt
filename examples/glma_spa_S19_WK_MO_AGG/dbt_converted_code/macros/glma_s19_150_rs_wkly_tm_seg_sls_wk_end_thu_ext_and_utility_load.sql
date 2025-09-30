{% macro refresh_wkly_tm_seg_sls(schema_name) %}

  {%- set date_query %}
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
  {%- endset %}

  {% set date_range = run_query(date_query) %}
  {% if execute %}
    {% set wk_from_dt = date_range.columns[0].values()[0] %}
    {% set wk_to_dt = date_range.columns[1].values()[0] %}
  {% else %}
    {% set wk_from_dt = '1970-01-01' %}
    {% set wk_to_dt = '1970-01-01' %}
  {% endif %}

  -- Debug info (optional)
  {{ log("Deleting and inserting data between " ~ wk_from_dt ~ " and " ~ wk_to_dt, info=True) }}

  -- Delete existing data in date range
  delete from `{{ schema_name }}.WKLY_TM_SEG_SLS_WK_END_THU`
  where WK_END_THU_ID_NU in (
    select distinct WK_END_THU_ID_NU
    from `{{ schema_name }}.cal_dt`
    where cal_dt between date('{{ wk_from_dt }}') and date('{{ wk_to_dt }}')
  );

  -- Insert aggregated data
  insert into `{{ schema_name }}.WKLY_TM_SEG_SLS_WK_END_THU` (
    WK_END_THU_ID_NU,
    MCD_GBAL_LCAT_ID_NU,
    TM_SEG_ID_NU,
    SALE_TYP_ID_NU,
    CURN_ISO_NU,
    WKLY_TM_SEG_NET_SLS_AM,
    WKLY_TM_SEG_TRN_CNT_QT,
    TM_SEG_SLS_DY_CNT_QT,
    WKLY_TM_NON_REPT_HLDY_QT
  )
  select
    cast(div(date_diff(a.cal_dt, date '1970-01-02', day), 7) + 2 as int64) as WK_END_THU_ID_NU,
    a.MCD_GBAL_LCAT_ID_NU,
    a.TM_SEG_ID_NU,
    a.SALE_TYP_ID_NU,
    a.CURN_ISO_NU,
    cast(sum(a.NET_SLS_AM) as numeric(18,2)) as WKLY_TM_SEG_NET_SLS_AM,
    cast(sum(a.TRN_CNT_QT) as int64) as WKLY_TM_SEG_TRN_CNT_QT,
    cast(count(*) as int64) as TM_SEG_SLS_DY_CNT_QT,
    cast(coalesce(c.WK_END_THU_HLDY_DY_CNT_QT, 0) - sum(case when b.cal_dt is null then 0 else 1 end) as int64) as WKLY_TM_NON_REPT_HLDY_QT
  from `{{ schema_name }}.dy_tm_seg_sls` a
  inner join `{{ schema_name }}.mcd_gbal_busn_lcat` a1
    on a1.MCD_GBAL_LCAT_ID_NU = a.MCD_GBAL_LCAT_ID_NU
  left join `{{ schema_name }}.CTRY_HLDY` b
    on b.CTRY_ISO_NU = a1.CTRY_ISO_NU
    and b.cal_dt = a.cal_dt
  left join (
    select
      a.WK_END_THU_ID_NU,
      b.CTRY_ISO_NU,
      count(*) as WK_END_THU_HLDY_DY_CNT_QT
    from `{{ schema_name }}.cal_dt` a
    inner join `{{ schema_name }}.CTRY_HLDY` b
      on b.cal_dt = a.cal_dt
    group by a.WK_END_THU_ID_NU, b.CTRY_ISO_NU
  ) c
    on c.WK_END_THU_ID_NU = cast(div(date_diff(a.cal_dt, date '1970-01-02', day), 7) + 2 as int64)
    and c.CTRY_ISO_NU = a1.CTRY_ISO_NU
  where a.cal_dt between date('{{ wk_from_dt }}') and date('{{ wk_to_dt }}')
    and a1.CTRY_ISO_NU = 840
  group by
    WK_END_THU_ID_NU,
    a.MCD_GBAL_LCAT_ID_NU,
    a.TM_SEG_ID_NU,
    a.SALE_TYP_ID_NU,
    a.CURN_ISO_NU,
    c.WK_END_THU_HLDY_DY_CNT_QT;

{% endmacro %}
