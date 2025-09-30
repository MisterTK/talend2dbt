{% macro GSCV_S80_100_Inventory_OTI_Recon_Child_MERGE_RS_UPDT_dc_raw_itm_invn(target_schema='rmdw_tables', stage_schema='rmdw_stge') %}

MERGE INTO `{{ target_schema }}.dc_raw_itm_invn` AS target
USING `{{ stage_schema }}.dc_raw_itm_invn` AS b
ON (
    target.xtrn_raw_itm_nu = b.xtrn_raw_itm_nu AND
    IFNULL(NULLIF(target.gbal_trad_itm_nu, ''), '-') = IFNULL(NULLIF(b.gbal_trad_itm_nu, ''), '-') AND
    target.dc_wsi_nu = b.dc_wsi_nu AND
    IFNULL(NULLIF(target.dc_gln_id, ''), '-') = IFNULL(NULLIF(b.dc_gln_id, ''), '-') AND
    target.dc_terr_cd = b.dc_terr_cd AND
    target.invn_aval_to_ship_dt = b.invn_aval_to_ship_dt AND
    target.invn_aval_typ_cd = b.invn_aval_typ_cd AND
    IFNULL(NULLIF(CAST(target.fcil_wsi_nu AS STRING), ''), '-') = IFNULL(NULLIF(CAST(b.fcil_wsi_nu AS STRING), ''), '-') AND
    IFNULL(NULLIF(target.fcil_gln_id, ''), '-') = IFNULL(NULLIF(b.fcil_gln_id, ''), '-') AND
    IFNULL(NULLIF(CAST(target.supp_wsi_nu AS STRING), ''), '-') = IFNULL(NULLIF(CAST(b.supp_wsi_nu AS STRING), ''), '-') AND
    IFNULL(NULLIF(target.supp_gln_id, ''), '-') = IFNULL(NULLIF(b.supp_gln_id, ''), '-') AND
    target.raw_itm_lcl_fl = b.raw_itm_lcl_fl
)

WHEN MATCHED THEN
  UPDATE SET
    xtrn_raw_itm_nu      = b.xtrn_raw_itm_nu,
    gbal_trad_itm_nu     = b.gbal_trad_itm_nu,
    dc_wsi_nu            = b.dc_wsi_nu,
    dc_gln_id            = b.dc_gln_id,
    dc_terr_cd           = b.dc_terr_cd,
    invn_aval_to_ship_dt = b.invn_aval_to_ship_dt,
    invn_aval_typ_cd     = b.invn_aval_typ_cd,
    fcil_wsi_nu          = b.fcil_wsi_nu,
    fcil_gln_id          = b.fcil_gln_id,
    supp_wsi_nu          = b.supp_wsi_nu,
    supp_gln_id          = b.supp_gln_id,
    invn_cases_qt        = b.invn_cases_qt,
    raw_itm_lcl_fl       = b.raw_itm_lcl_fl,
    updt_dw_audt_ts      = b.updt_dw_audt_ts,
    dw_file_id           = b.dw_file_id

WHEN NOT MATCHED THEN
  INSERT (
    xtrn_raw_itm_nu,
    gbal_trad_itm_nu,
    dc_wsi_nu,
    dc_gln_id,
    dc_terr_cd,
    invn_aval_to_ship_dt,
    invn_aval_typ_cd,
    fcil_wsi_nu,
    fcil_gln_id,
    supp_wsi_nu,
    supp_gln_id,
    invn_cases_qt,
    raw_itm_lcl_fl,
    load_dw_audt_ts,
    updt_dw_audt_ts,
    dw_file_id
  )
  VALUES (
    b.xtrn_raw_itm_nu,
    b.gbal_trad_itm_nu,
    b.dc_wsi_nu,
    b.dc_gln_id,
    b.dc_terr_cd,
    b.invn_aval_to_ship_dt,
    b.invn_aval_typ_cd,
    b.fcil_wsi_nu,
    b.fcil_gln_id,
    b.supp_wsi_nu,
    b.supp_gln_id,
    b.invn_cases_qt,
    b.raw_itm_lcl_fl,
    b.load_dw_audt_ts,
    b.updt_dw_audt_ts,
    b.dw_file_id
  );

{% endmacro %}
