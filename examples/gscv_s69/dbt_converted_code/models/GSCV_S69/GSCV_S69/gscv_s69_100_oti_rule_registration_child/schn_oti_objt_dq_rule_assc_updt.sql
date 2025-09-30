{{ config(
    materialized = 'view',
    alias = 'schn_oti_objt_dq_rule_assc_updt',
    tags = ['gscv_s69_oti_rule_registration_grandmaster','gscv_s69_100_oti_rule_registration']
) }}

WITH source_row1 AS (
    SELECT
        fileorigin,
        ErroCode,
        Content,
        Column_position,
        Datatype,
        length,
        rulematch,
        Lookup_Table_nm,
        Lookup_FK,
        Lookup_filter,
        Schema_check_type,
        Rule,
        PRIORITY,
        Errordescription,
        srce_file_clmn_lkup_tble_na,
        srce_file_clmn_lkup_clmn_na
    FROM {{ source('rmdw_tables', 'GSCV_SC_OTI_DQ_Rules_csv') }} 
),
source_row3 AS (
    SELECT
        schn_oti_objt_dq_rule_assc_id,
        schn_oti_objt_na,
        schn_dq_rule_id
    FROM {{ source('Redshift_gdap_data_audit', 'schn_oti_objt_dq_rule_assc') }} 
)

SELECT
    COALESCE(row3.schn_oti_objt_dq_rule_assc_id, 0) AS schn_oti_objt_dq_rule_assc_id,
    row1.fileorigin AS schn_oti_objt_na,
    row1.ErroCode AS schn_dq_rule_id,
    CASE WHEN row1.Content = '' THEN '-' ELSE row1.Content END AS srce_file_clmn_na,
    CASE WHEN row1.Column_position = '' THEN '-' ELSE row1.Column_position END AS srce_file_clmn_pstn_tx,
    CASE WHEN row1.Datatype = '' THEN '-' ELSE row1.Datatype END AS srce_file_clmn_xpct_data_typ_na,
    CASE WHEN row1.length = '' THEN '-' ELSE row1.length END AS srce_file_clmn_xpct_len_tx,
    CASE WHEN row1.rulematch = '' THEN '-' ELSE row1.rulematch END AS srce_file_clmn_rule_mtch_tx,
    CASE WHEN row1.Lookup_Table_nm = '' THEN '-' ELSE row1.Lookup_Table_nm END AS srce_file_clmn_lkup_dsn,
    CASE WHEN row1.Lookup_FK = '' THEN '-' ELSE row1.Lookup_FK END AS srce_file_clmn_lkup_data_set_fk_na,
    CASE WHEN row1.Lookup_filter = '' THEN '-' ELSE row1.Lookup_filter END AS srce_file_clmn_lkup_data_set_fltr_na,
    CASE WHEN row1.Schema_check_type = '' THEN '-' ELSE row1.Schema_check_type END AS srce_file_clmn_vld_typ_na,
    CASE WHEN row1.Rule = '' THEN '-' ELSE row1.Rule END AS err_msg_tx,
    CASE WHEN row1.PRIORITY = '' THEN '-' ELSE row1.PRIORITY END AS xcpt_sevr_cd,
    CASE WHEN row1.Rule = '' THEN '-' ELSE row1.Rule END AS dq_rule_tech_ds,
    '-1' AS dw_file_id,
    row1.Errordescription AS err_msg_cat_tx,
    CASE WHEN row1.srce_file_clmn_lkup_tble_na = '' THEN '-' ELSE row1.srce_file_clmn_lkup_tble_na END AS srce_file_clmn_lkup_tble_na,
    CASE WHEN row1.srce_file_clmn_lkup_clmn_na = '' THEN '-' ELSE row1.srce_file_clmn_lkup_clmn_na END AS srce_file_clmn_lkup_clmn_na
FROM
    source_row1 AS row1
LEFT JOIN
    source_row3 AS row3
ON
    row1.fileorigin = row3.schn_oti_objt_na
    AND row1.ErroCode = row3.schn_dq_rule_id