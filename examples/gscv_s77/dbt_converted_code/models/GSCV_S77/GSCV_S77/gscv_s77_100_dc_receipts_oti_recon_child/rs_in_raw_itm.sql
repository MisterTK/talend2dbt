{{ config(
    materialized='table',
    alias='rs_in_raw_itm', 
    tags=['gscv_s77_dc_receipts_oti_recon_grandmaster','gscv_s77_100_dc_receipts_oti_recon_child']
) }}

SELECT
    terr_cd,
    xtrn_raw_itm_nu
FROM
    {{ source('Redshift_gdap_Schema', 'raw_itm') }}