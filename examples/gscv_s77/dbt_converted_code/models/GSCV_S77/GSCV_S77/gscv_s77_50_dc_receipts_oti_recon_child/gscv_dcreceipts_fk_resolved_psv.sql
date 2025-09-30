{{ config(
    materialized='table', 
    alias='gscv_dcreceipts_fk_resolved_psv', 
    post_hook=[
      """
      UPDATE {{ var('Redshift_gdap_data_audit_schema') }}.oti_dc_rcpt_rjct AS a
      SET
          rsol_dt = b.rsol_dt,
          cmnt_tx = b.cmnt_tx,
          updt_dw_audt_ts = CURRENT_TIMESTAMP()
      FROM
          {{ var('Redshift_gdap_Stage') }}.oti_dc_rcpt_rjct AS b
      WHERE
          a.OTI_DC_RCPT_RJCT_ID = b.OTI_DC_RCPT_RJCT_ID
          AND a.ERR_CD = b.ERR_CD;
      """
    ],
    tags=['gscv_s77_dc_receipts_oti_recon_grandmaster','gscv_s77_50_dc_receipts_oti_recon_child']
) }}

SELECT
    t1.oti_dc_rcpt_rjct_id ,
    t1.err_cd,
    t1.item_number,
    t1.wsi,
    t1.dc,
    t1.countrycode,
    t1.transdc,
    CAST(t1.daterec AS date) AS daterc,
    t1.cases,
    t1.fcacost,
    t1.fcacurr,
    t1.freedc,
    t1.freedccurr,
    t1.poline,
    t1.ponum,
    CAST(t1.transdate AS date) AS transdate,
    t1.localflag,
    t1.gtin,
    t1.transdc_gln,
    t1.facility_gln,
    t1.dc_gln,
    CAST(t1.terr_cd AS INT64) AS terr_cd,
    CAST(NULL AS TIMESTAMP) AS srce_file_recv_ts,
    CAST(NULL AS TIMESTAMP) AS fst_occr_dt,
    CAST(NULL AS TIMESTAMP) AS ltst_reoc_dt,
    CAST(NULL AS INT64) AS cnt_of_occr_nu,
    CURRENT_TIMESTAMP() AS rsol_dt,
    CAST(NULL AS STRING) AS sevr_typ,
    CAST(NULL AS STRING) AS err_ds,
    '{{ var("cmnt_value", "Default Comment") }}' AS cmnt_tx,
    CAST(NULL AS INT64) AS rec_serl_nu,
    t1.srce_file_nm,
    t1.dw_file_id,
    CAST(t1.oti_dq_srce_err_typ_id AS INT64) AS oti_dq_srce_err_typ_id,
    CAST(t1.oti_dq_rspn_typ_id AS INT64) AS oti_dq_rspn_typ_id
FROM
     {{ ref('gscv_dcreceipts_fk_unresolved_psv_tmap1') }}  AS t1

INNER JOIN
    {{ ref('fk_oti_recon_rejects_gscv_dcreceipts_fk_source1_final_psv') }}  AS t2
    ON t1.oti_dc_rcpt_rjct_id = t2.oti_dc_rcpt_rjct_id
    AND t1.err_cd = t2.issue_code