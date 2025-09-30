{{ config(
    materialized='table',
    alias='rs_in_shpg_unt',
    tags=['gscv_s77_dc_receipts_oti_recon_grandmaster','gscv_s77_100_dc_receipts_oti_recon_child']
) }}

SELECT
    SHPG_UNT_NU,
    XTRN_RAW_ITM_NU
FROM
    {{ source('Redshift_gdap_Schema', 'shpg_unt') }}
WHERE
    EXTRACT(YEAR FROM shpg_unt_end_dt) = 9999
    AND terr_cd = 0