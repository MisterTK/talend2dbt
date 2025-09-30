{{ config(
    materialized = 'view',
    alias = 'schn_oti_objt_dq_rule_assc_new',
    tags = ['gscv_s69_oti_rule_registration_grandmaster','gscv_s69_100_oti_rule_registration']
) }}


SELECT
    
    fileorigin AS schn_oti_objt_na, -- Maps to schn_oti_objt_na
    ErroCode AS schn_dq_rule_id,     -- Maps to schn_dq_rule_id

    -- Applying CASE WHEN for empty string replacements as per Talend's logic
    CASE WHEN Content = '' THEN '-' ELSE Content END AS srce_file_clmn_na,
    CASE WHEN Column_position = '' THEN '-' ELSE Column_position END AS srce_file_clmn_pstn_tx,
    CASE WHEN Datatype = '' THEN '-' ELSE Datatype END AS srce_file_clmn_xpct_data_typ_na,
    CASE WHEN length = '' THEN '-' ELSE length END AS srce_file_clmn_xpct_len_tx,
    CASE WHEN rulematch = '' THEN '-' ELSE rulematch END AS srce_file_clmn_rule_mtch_tx,
    CASE WHEN Lookup_Table_nm = '' THEN '-' ELSE Lookup_Table_nm END AS srce_file_clmn_lkup_dsn,
    CASE WHEN Lookup_FK = '' THEN '-' ELSE Lookup_FK END AS srce_file_clmn_lkup_data_set_fk_na,
    CASE WHEN Lookup_filter = '' THEN '-' ELSE Lookup_filter END AS srce_file_clmn_lkup_data_set_fltr_na,
    CASE WHEN Schema_check_type = '' THEN '-' ELSE Schema_check_type END AS srce_file_clmn_vld_typ_na,
    CASE WHEN Rule = '' THEN '-' ELSE Rule END AS err_msg_tx,
    CASE WHEN PRIORITY = '' THEN '-' ELSE PRIORITY END AS xcpt_sevr_cd,
    CASE WHEN Rule = '' THEN '-' ELSE Rule END AS dq_rule_tech_ds,
    '-1' AS dw_file_id, -- Hardcoded value from Talend tMap
    Errordescription AS err_msg_cat_tx,
    CASE WHEN srce_file_clmn_lkup_tble_na = '' THEN '-' ELSE srce_file_clmn_lkup_tble_na END AS srce_file_clmn_lkup_tble_na,
    CASE WHEN srce_file_clmn_lkup_clmn_na = '' THEN '-' ELSE srce_file_clmn_lkup_clmn_na END AS srce_file_clmn_lkup_clmn_na

FROM
    {{ source('rmdw_tables', 'GSCV_SC_OTI_DQ_Rules_csv') }}

-- Removed the 'is_incremental()' block because this model is a view,
-- and incremental logic should be handled in the final 'fct' model.