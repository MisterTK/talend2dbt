{{
    config(
        materialized = 'incremental',
        tags = ['gscv_oti_s26_dcreceipts_grandmaster','gscv_oti_s26_50_dly_dc_rcpt_dq_checks_child'],
        alias = 'dly_dc_rcpt_gscv_dq_oti_reject_final_insert2_psv',
        post_hook=["{% do reusable_js_s3_redshift_copy_withdynamicschema_new_api(this.schema,'rmdw_stge' ,'oti_dc_rcpt_rjct', this.identifier, 1) %}"]
    )
}}

WITH tUniqRow_2 AS (
      SELECT *,
  ROW_NUMBER() OVER (PARTITION BY
           err_cd,item_number,wsi,dc,countrycode,transdc,daterc,cases,
           fcacost,fcacurr,freedc,freedccurr,poline,ponum,transdate,
           localflag,gtin,transdc_gln,facility_gln,dc_gln,terr_cd
      ) AS rn
      FROM {{ ref('gscv_dq_oti_reject_final_insert2appended_psv') }}
)
SELECT * EXCEPT(rn) FROM tUniqRow_2 WHERE rn = 1