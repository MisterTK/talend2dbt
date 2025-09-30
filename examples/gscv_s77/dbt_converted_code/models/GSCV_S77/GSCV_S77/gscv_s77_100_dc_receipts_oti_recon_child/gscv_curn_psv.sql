{{ config(
    materialized='view', 
    alias='gscv_curn_psv',
    tags=['gscv_s77_dc_receipts_oti_recon_grandmaster','gscv_s77_100_dc_receipts_oti_recon_child']
) }}

SELECT
    curn_iso_nu,
    curn_iso3_abbr_cd,
    curn_ds,
    euro_fix_xcng_rate_nu,
    trdd_in_us_dllr_fl,
    load_dw_audt_ts,
    updt_dw_audt_ts,
    dw_file_id
FROM
    {{ source('Redshift_gdap_Schema', 'curn') }}