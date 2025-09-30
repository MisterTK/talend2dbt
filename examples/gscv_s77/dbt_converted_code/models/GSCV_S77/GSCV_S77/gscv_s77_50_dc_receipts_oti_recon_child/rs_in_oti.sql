{{ config(
    materialized='view',
    cluster_by=['oti_updt_dw_audt_ts'],
    alias='rs_in_oti',
    tags=['gscv_s77_dc_receipts_oti_recon_grandmaster','gscv_s77_50_dc_receipts_oti_recon_child']
) }}

WITH oti_data_step1 AS (
    SELECT
        a.srce_file_clmn_vld_typ_na,
        e.updt_dw_audt_ts AS oti_updt_dw_audt_ts,
        e.oti_dc_rcpt_rjct_id,
        e.err_cd,
        e.sevr_typ,
        e.terr_cd,
        e.srce_file_nm,
        e.dw_file_id,
        e.item_number,
        e.wsi,
        e.dc,
        e.countrycode,
        e.transdc,
        e.daterc,
        e.cases,
        e.fcacost,
        e.fcacurr,
        e.freedc,
        e.freedccurr,
        e.ponum,
        e.poline,
        e.transdate,
        e.localflag,
        e.gtin,
        e.dc_gln,
        e.transdc_gln,
        e.facility_gln,
        e.oti_dq_srce_err_typ_id,
        e.oti_dq_rspn_typ_id,
        CASE
            WHEN a.srce_file_clmn_vld_typ_na = 'Data_Integrity' AND e.sevr_typ = 'CRITICAL' THEN 2
            ELSE 1
        END AS flag 
    FROM
        {{ source('Redshift_gdap_data_audit', 'oti_dc_rcpt_rjct') }} e
    INNER JOIN
        {{ source('Redshift_gdap_data_audit', 'schn_oti_objt_dq_rule_assc') }} a
        ON a.schn_dq_rule_id = e.err_cd
    WHERE
        DATE(e.updt_dw_audt_ts) > '{{ var("from_dt") }}'
        AND DATE(e.updt_dw_audt_ts) <= '{{ var("to_dt") }}'
        AND e.sevr_typ IN ({{ var("sevr_typ") }})
        AND a.srce_file_clmn_vld_typ_na NOT IN ('Schem_validation', 'Data_Integrity')
        AND a.schn_dq_rule_id NOT IN ({{ var("exclude_err_cd") }})
        AND e.rsol_dt IS NULL
),
oti_data AS (
    SELECT
        *, 
        MAX(flag) OVER (PARTITION BY oti_dc_rcpt_rjct_id) AS consider 
    FROM
        oti_data_step1
)
SELECT
    CASE WHEN ddr.updt_dw_audt_ts IS NULL THEN CAST('0001-01-01 00:00:00' AS DATETIME) ELSE ddr.updt_dw_audt_ts END AS ddr_updt_dw_audt_ts,
    oti.oti_updt_dw_audt_ts,
    oti.oti_dc_rcpt_rjct_id,
    oti.err_cd,
    oti.sevr_typ,
    oti.terr_cd,
    oti.srce_file_nm,
    oti.dw_file_id,
    oti.item_number,
    oti.wsi,
    oti.dc,
    oti.countrycode,
    oti.transdc,
    oti.daterc,
    oti.cases,
    oti.fcacost,
    oti.fcacurr,
    oti.freedc,
    oti.freedccurr,
    oti.ponum,
    oti.poline,
    oti.transdate,
    oti.localflag,
    oti.gtin,
    oti.dc_gln,
    oti.transdc_gln,
    oti.facility_gln,
    oti.oti_dq_srce_err_typ_id,
    oti.oti_dq_rspn_typ_id
FROM
    oti_data oti
LEFT JOIN
    {{ source('Redshift_gdap_Schema', 'shpg_unt') }} shpg
    ON shpg.xtrn_raw_itm_nu = oti.item_number
    AND EXTRACT(YEAR FROM shpg.shpg_unt_end_dt) = 9999 
    AND shpg.terr_cd = 0
LEFT JOIN
{{ source('Redshift_gdap_Schema', 'dly_dc_rcpt') }} ddr
ON ddr.dc_terr_cd = CAST(oti.countrycode AS INT)
AND ddr.dc_rcpt_dt = PARSE_DATE('%d%m%Y', LPAD(CAST(oti.daterc AS STRING), 8, '0')) 
AND ddr.po_id = oti.ponum
AND ddr.po_ln_id = CAST(oti.poline AS INT)
AND ddr.dc_fcil_id = CAST(oti.dc AS INT)
AND ddr.wsi_nu = CAST(oti.wsi AS INT) 
AND ddr.shpg_unt_nu = shpg.shpg_unt_nu
WHERE
    oti.consider <> 2
ORDER BY
    oti_updt_dw_audt_ts DESC