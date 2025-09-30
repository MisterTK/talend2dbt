{{ config(
    materialized = 'view',
    alias = 'schn_oti_objt_dq_rules_assc_tmap2',
    tags = ['gscv_s69_oti_rule_registration_grandmaster','gscv_s69_100_oti_rule_registration']
) }}


SELECT
    COALESCE(MAX(schn_oti_objt_dq_rule_assc_id), 0) AS max_schn_oti_objt_dq_rule_assc_id
FROM
    {{ source('Redshift_gdap_data_audit', 'schn_oti_objt_dq_rule_assc') }}
    