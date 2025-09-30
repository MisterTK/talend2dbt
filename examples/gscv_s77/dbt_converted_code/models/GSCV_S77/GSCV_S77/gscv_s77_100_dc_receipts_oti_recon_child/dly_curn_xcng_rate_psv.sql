{{ config(
    materialized='view',
    alias='dly_curn_xcng_rate_psv',
    tags=['gscv_s77_dc_receipts_oti_recon_grandmaster','gscv_s77_100_dc_receipts_oti_recon_child']
) }}

SELECT
    cal_dt,
    curn_iso_nu,
    from_curn_iso_nu,
    dly_xcng_rate_nu
FROM
    {{ source('Redshift_gdap_Schema', 'dly_curn_xcng_rate') }}
WHERE
    EXTRACT(YEAR FROM cal_dt) > 2016
