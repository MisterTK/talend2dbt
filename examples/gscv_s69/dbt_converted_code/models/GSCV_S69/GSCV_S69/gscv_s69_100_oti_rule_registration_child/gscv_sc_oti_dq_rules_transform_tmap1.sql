{{ config(
    materialized = 'ephemeral',
    alias = 'gscv_sc_oti_dq_rules_transform_tmap1',
    tags = ['gscv_s69_oti_rule_registration_grandmaster','gscv_s69_100_oti_rule_registration']
) }}

SELECT DISTINCT
  a.Column_position,
  a.Content,
  a.Datatype,
  a.ErroCode,
  a.Errordescription,
  a.Lookup_FK,
  a.Lookup_Table_nm,
  a.Lookup_filter,
  a.PRIORITY,
  a.Rejectrecords,
  a.Rule,
  a.Schema_check_type,
  a.StoreinDCReceipttable,
  a.Storeinrejectiontable,
  a.fileorigin,
  a.length,
  a.rulematch,
  a.srce_file_clmn_lkup_clmn_na,
  a.srce_file_clmn_lkup_tble_na,
  b.schn_dq_rule_id,
  b.schn_oti_objt_dq_rule_assc_id,
  b.schn_oti_objt_na
FROM {{ source('rmdw_tables', 'GSCV_SC_OTI_DQ_Rules_csv') }} AS a
INNER JOIN {{ source('Redshift_gdap_data_audit', 'schn_oti_objt_dq_rule_assc') }} AS b ON a.fileorigin = b.schn_oti_objt_na and a.ErroCode = b.schn_dq_rule_id;
