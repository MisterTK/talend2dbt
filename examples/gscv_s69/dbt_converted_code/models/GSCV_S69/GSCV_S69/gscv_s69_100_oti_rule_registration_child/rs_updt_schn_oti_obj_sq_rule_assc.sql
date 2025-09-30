{{config(
        materialized='incremental',
        schema=var('Redshift_gdap_data_audit_schema'),
        alias='schn_oti_objt_dq_rule_assc',
        unique_key=['schn_oti_objt_dq_rule_assc_id', 'schn_oti_objt_na'],
        incremental_strategy='merge',
        tags=['gscv_s69_oti_rule_registration_grandmaster','gscv_s69_100_oti_rule_registration'],
        post_hook="TRUNCATE TABLE {{ source('Redshift_gdap_Stage', 'schn_oti_objt_dq_rule_assc') }}"
    )
}}

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
    load_dw_audt_ts,
    updt_dw_audt_ts,
    dw_file_id,
    err_msg_cat_tx,
    srce_file_clmn_lkup_tble_na,
    srce_file_clmn_lkup_clmn_na
FROM
   
    {{ source('Redshift_gdap_Stage', 'schn_oti_objt_dq_rule_assc') }}

{% if is_incremental() %}
    -- Filter for new or updated records from the staging table
    -- This 'WHERE' clause is critical for efficient incremental builds
    WHERE updt_dw_audt_ts > (SELECT MAX(updt_dw_audt_ts) FROM {{ this }})
{% endif %}