{% macro insert_mo_tm_seg_sls() %}

insert into `{{ var('Redshift_gdap_Schema') }}.mo_tm_seg_sls` (
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
select
    YR_NU,
    MO_NU,
    MCD_GBAL_LCAT_ID_NU,
    TM_SEG_ID_NU,
    SALE_TYP_ID_NU,
    CURN_ISO_NU,
    MO_TM_SEG_NET_SLS_AM,
    MO_TM_SEG_TRN_CNT_QT,
    MO_TM_SEG_SLS_DY_QT
from `{{ var('Redshift_gdap_Utility') }}.mo_tm_seg_sls`;

{% endmacro %}
