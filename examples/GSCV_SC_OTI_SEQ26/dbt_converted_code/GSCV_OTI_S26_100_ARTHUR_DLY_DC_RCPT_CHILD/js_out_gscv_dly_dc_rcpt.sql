{{
    config(
        materialized = 'table',
        tags = ['gscv_oti_s26_dcreceipts_grandmaster','gscv_oti_s26_100_arthur_dly_dc_rcpt_child'],
        alias = 'gscv_dly_dc_rcpt',
        post_hook=["{% do reusable_js_s3_redshift_copy_withdynamicschema_new_api(this.schema,'rmdw_stge' ,'stg_dly_dc_rcpt', this.identifier, 1) %}"]
    )
}}

WITH SortUnique AS (
SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY
                dc_fcil_id,
                dc_terr_cd,
                po_id,
                po_ln_id,
                wsi_nu,
                shpg_unt_nu,
                dc_rcpt_dt
        ORDER BY Timestamp DESC) AS rn 
FROM {{ ref('js_out_gscv_dly_dc_rcpt_append') }}
)
SELECT * EXCEPT(rn,Timestamp) FROM SortUnique WHERE rn = 1