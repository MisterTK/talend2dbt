-- macros/mo_dypt_sls_insert.sql
{% macro mo_dypt_sls_insert() %}

insert into {{ var('Redshift_gdap_Schema') }}.mo_dypt_sls (
    YR_NU,
    MO_NU,
    MCD_GBAL_LCAT_ID_NU,
    DYPT_ID_NU,
    SALE_TYP_ID_NU,
    CURN_ISO_NU,
    MO_DYPT_NET_SLS_AM,
    MO_DYPT_TRN_CNT_QT,
    MO_DYPT_SLS_DY_QT
)
select
    YR_NU,
    MO_NU,
    MCD_GBAL_LCAT_ID_NU,
    DYPT_ID_NU,
    SALE_TYP_ID_NU,
    CURN_ISO_NU,
    MO_DYPT_NET_SLS_AM,
    MO_DYPT_TRN_CNT_QT,
    MO_DYPT_SLS_DY_QT
from {{  var('Redshift_gdap_Utility') }}.mo_dypt_sls

{% endmacro %}
