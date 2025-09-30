{{
    config(
        materialized='table', 
        alias='gscv_dly_dc_rcpt_etl_rejects_psv',
        tags=['gscv_s77_dc_receipts_oti_recon_grandmaster','gscv_s77_150_dc_receipts_oti_recon_child'],
        pre_hook="""
            CREATE OR REPLACE EXTERNAL TABLE
            `dmgcp-del-155.rmdw_tables.gscv_dly_dc_rcpt_dly_xcng_rate_rej_psv`(
                wrin STRING,
                dc_fcil_id STRING,
                dc_terr_cd STRING,
                dc_rcpt_xtrc_dt STRING,
                po_id STRING,
                po_ln_id STRING,
                wsi STRING,
                shpg_unt_nu STRING,
                fca_lcl_curn_iso_nu STRING,
                fdc_lcl_curn_iso_nu STRING,
                dc_rcpt_dt STRING,
                dc_trsf_fcil_id STRING,
                shpg_unt_case_recv_qt STRING,
                fca_case_cost_am STRING,
                fdc_case_cost_am STRING,
                raw_itm_lcl_fl STRING,
                gbal_fca_case_cost_am STRING,
                gbal_fdc_case_cost_am STRING,
                gbal_curn_iso_nu STRING,
                dw_file_id STRING,
                dc_gln_id STRING,
                dc_trsf_fcil_gln_id STRING,
                fcil_gln_id STRING,
                dc_pri_gln_id STRING,
                dc_pri_trsf_fcil_gln_id STRING,
                fcil_pri_gln_id STRING,
                gbal_trad_itm_nu STRING,
                euro_fca_case_cost_am STRING,
                euro_fdc_case_cost_am STRING,
                euro_curn_iso_nu STRING,
                Timestamp DATE,
                terr_cd_mapped INT64,
                err_cd STRING,
                fcacurr STRING,
                dccurr STRING,
                srce_file_nm STRING,
                oti_dc_rcpt_rjct_id STRING,
                oti_dq_srce_err_typ_id INT64,
                oti_dq_rspn_typ_id INT64
            )
            OPTIONS (
                format = 'csv',
                uris = ['gs://dmgcp-del-155-data/GSCV_TEST/GSCV_DLY_DC_RCPT_DLY_XCNG_RATE_REJ_psv.psv'],
                skip_leading_rows = 1,
                field_delimiter ='|'
            )"""
    )}}


select * from  {{ source('rmdw_tables', 'gscv_dly_dc_rcpt_dly_xcng_rate_rej_psv') }}