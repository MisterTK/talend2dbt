{{ config(
    materialized='table',
    alias='js_outgscv_sc_oti_dq_rules_psv',
    description='Merges multiple data sources for data quality rule associations',
    post_hook=["{% do REUSABLE_JS_S3_REDSHIFT_copy_withdynamicschema_new_API(this.schema, 'schn_oti_objt_dq_rule_assc', this.identifier, 1) %}"],
    tags = ['gscv_s69_oti_rule_registration_grandmaster','gscv_s69_100_oti_rule_registration']
) }}

SELECT 
    schn_oti_objt_dq_rule_assc_id,
    schn_oti_objt_na,
    schn_dq_rule_id,
    srce_file_clmn_na,
    srce_file_clmn_pstn_tx,
    srce_file_clmn_xpct_data_typ_na,
    srce_file_clmn_xpct_len_tx,
    srce_file_clmn_rule_mtch_tx,
    srce_file_clmn_lkup_dsn,
    srce_file_clmn_lkup_data_set_fk_na,
    srce_file_clmn_lkup_data_set_fltr_na,
    srce_file_clmn_vld_typ_na,
    err_msg_tx,
    xcpt_sevr_cd,
    dq_rule_tech_ds,
    dw_file_id,
    err_msg_cat_tx,
    srce_file_clmn_lkup_tble_na,
    srce_file_clmn_lkup_clmn_na
FROM {{ ref('schn_oti_objt_dq_rule_assc_new_final') }}

UNION ALL

SELECT 
    schn_oti_objt_dq_rule_assc_id,
    schn_oti_objt_na,
    schn_dq_rule_id,
    srce_file_clmn_na,
    srce_file_clmn_pstn_tx,
    srce_file_clmn_xpct_data_typ_na,
    srce_file_clmn_xpct_len_tx,
    srce_file_clmn_rule_mtch_tx,
    srce_file_clmn_lkup_dsn,
    srce_file_clmn_lkup_data_set_fk_na,
    srce_file_clmn_lkup_data_set_fltr_na,
    srce_file_clmn_vld_typ_na,
    err_msg_tx,
    xcpt_sevr_cd,
    dq_rule_tech_ds,
    dw_file_id,
    err_msg_cat_tx,
    srce_file_clmn_lkup_tble_na,
    srce_file_clmn_lkup_clmn_na
FROM {{ ref('schn_oti_objt_dq_rule_assc_updt') }}
