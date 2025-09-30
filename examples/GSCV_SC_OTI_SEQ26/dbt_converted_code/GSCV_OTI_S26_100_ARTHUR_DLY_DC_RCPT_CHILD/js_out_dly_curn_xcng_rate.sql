{{
    config(
        materialized = 'table',
        tags = ['gscv_oti_s26_dcreceipts_grandmaster','gscv_oti_s26_100_arthur_dly_dc_rcpt_child'],
        alias = 'dly_curn_xcng_rate'
    )
}}
-- done
SELECT 
    cal_dt,
	curn_iso_nu,
	from_curn_iso_nu,
	dly_xcng_rate_nu
FROM {{ source('rmdw_tables','dly_curn_xcng_rate') }}
WHERE EXTRACT(YEAR FROM cal_dt) > 2016 