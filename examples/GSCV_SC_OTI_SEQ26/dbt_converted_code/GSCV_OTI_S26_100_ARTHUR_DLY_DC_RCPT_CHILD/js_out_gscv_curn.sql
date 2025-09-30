{{
    config(
        materialized = 'table',
        tags = ['gscv_oti_s26_dcreceipts_grandmaster','gscv_oti_s26_100_arthur_dly_dc_rcpt_child'],
        alias = 'gscv_curn'
    )
}}
--done
SELECT 
    curn_iso_nu,
	curn_iso3_abbr_cd,
	curn_ds,
	euro_fix_xcng_rate_nu,
	trdd_in_us_dllr_fl,
	load_dw_audt_ts,
	updt_dw_audt_ts,
	dw_file_id
 FROM {{ source('rmdw_tables','curn') }}
