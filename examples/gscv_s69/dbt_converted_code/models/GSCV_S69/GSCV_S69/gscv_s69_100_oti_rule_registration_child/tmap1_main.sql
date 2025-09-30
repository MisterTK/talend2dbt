{{ config(
    materialized = 'view',
    alias = 'tmap1_main',
    tags = ['gscv_s69_oti_rule_registration_grandmaster','gscv_s69_100_oti_rule_registration']
) }}

WITH source_data AS (
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
    FROM {{ source('rmdw_tables', 'GSCV_SC_OTI_DQ_Rules_csv') }} -- Replace with your actual source or model for row1
)

SELECT
    NULL AS schn_oti_objt_dq_rule_assc_id, -- Talend shows type id_Integer, but no expression, so defaulting to NULL or 0 based on downstream use
    source_data.fileorigin AS schn_oti_objt_na,
    source_data.ErroCode AS schn_dq_rule_id,
    CASE WHEN source_data.Content = '' THEN '-' ELSE source_data.Content END AS srce_file_clmn_na,
    CASE WHEN source_data.Column_position = '' THEN '-' ELSE source_data.Column_position END AS srce_file_clmn_pstn_tx,
    CASE WHEN source_data.Datatype = '' THEN '-' ELSE source_data.Datatype END AS srce_file_clmn_xpct_data_typ_na,
    CASE WHEN source_data.length = '' THEN '-' ELSE source_data.length END AS srce_file_clmn_xpct_len_tx,
    CASE WHEN source_data.rulematch = '' THEN '-' ELSE source_data.rulematch END AS srce_file_clmn_rule_mtch_tx,
    CASE WHEN source_data.Lookup_Table_nm = '' THEN '-' ELSE source_data.Lookup_Table_nm END AS srce_file_clmn_lkup_dsn,
    CASE WHEN source_data.Lookup_FK = '' THEN '-' ELSE source_data.Lookup_FK END AS srce_file_clmn_lkup_data_set_fk_na,
    CASE WHEN source_data.Lookup_filter = '' THEN '-' ELSE source_data.Lookup_filter END AS srce_file_clmn_lkup_data_set_fltr_na,
    CASE WHEN source_data.Schema_check_type = '' THEN '-' ELSE source_data.Schema_check_type END AS srce_file_clmn_vld_typ_na,
    CASE WHEN source_data.Rule = '' THEN '-' ELSE source_data.Rule END AS err_msg_tx,
    CASE WHEN source_data.PRIORITY = '' THEN '-' ELSE source_data.PRIORITY END AS xcpt_sevr_cd,
    CASE WHEN source_data.Rule = '' THEN '-' ELSE source_data.Rule END AS dq_rule_tech_ds,
    '-1' AS dw_file_id,
    source_data.Errordescription AS err_msg_cat_tx,
    CASE WHEN source_data.srce_file_clmn_lkup_tble_na = '' THEN '-' ELSE source_data.srce_file_clmn_lkup_tble_na END AS srce_file_clmn_lkup_tble_na,
    CASE WHEN source_data.srce_file_clmn_lkup_clmn_na = '' THEN '-' ELSE source_data.srce_file_clmn_lkup_clmn_na END AS srce_file_clmn_lkup_clmn_na
FROM
    source_data