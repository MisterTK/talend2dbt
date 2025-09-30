{{ config(
    materialized='incremental',
    alias='schn_oti_objt_dq_rule_assc_new_final',
    unique_key=['schn_oti_objt_na', 'schn_dq_rule_id', 'srce_file_clmn_na'],
    labels={
        'purpose': 'final_fact_dq_rule_assc'
    },
    tags=['gscv_s69_oti_rule_registration_grandmaster','gscv_s69_100_oti_rule_registration']
) }}


WITH base_records AS (
    SELECT *,
    DATE('{{ run_started_at.strftime("%Y-%m-%d") }}') AS load_date
    FROM {{ ref('schn_oti_objt_dq_rule_assc_new') }}
),

new_records AS (
    SELECT *
    FROM base_records
    {% if is_incremental() %}
    WHERE load_date = ('{{ run_started_at.strftime("%Y-%m-%d") }}')
    and NOT EXISTS (
        SELECT 1
        FROM {{ this }} AS target_table
        WHERE
            target_table.schn_oti_objt_na = base_records.schn_oti_objt_na
            AND target_table.schn_dq_rule_id = base_records.schn_dq_rule_id
            AND target_table.srce_file_clmn_na = base_records.srce_file_clmn_na
    )
    {% endif %}
),

max_id AS (
    SELECT max_schn_oti_objt_dq_rule_assc_id
    FROM {{ ref('schn_oti_objt_dq_rules_assc_tmap2') }}
),

assigned_ids AS (
    SELECT
        new_records.*,
        (SELECT max_schn_oti_objt_dq_rule_assc_id FROM max_id) +
        ROW_NUMBER() OVER (
            ORDER BY schn_oti_objt_na, schn_dq_rule_id, srce_file_clmn_na
        ) AS schn_oti_objt_dq_rule_assc_id
    FROM new_records
)

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
    srce_file_clmn_lkup_clmn_na,
    date(TIMESTAMP('{{ run_started_at }}')) AS load_date
FROM assigned_ids
