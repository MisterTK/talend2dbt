{{ config(
    materialized='ephemeral',
    tags = ['s89_itm_hrcy_grandmaster','s89_300_update_std_sld_menu_itm_lkup','s89_300_update_std_sld_menu_itm_lkup_intermediate']
) }}

SELECT
    smi.terr_cd,
    smi.sld_menu_itm_id,
    smi.gbal_menu_itm_id,
    gmi.gbal_menu_itm_na,
    smi.sld_menu_itm_na,
    smi.sld_menu_itm_shrt_na,
    smi.sld_menu_itm_ds,
    smi.sld_menu_itm_cert_typ_na,
    smi.sld_menu_itm_stus_cd,
    smi.sld_menu_itm_intd_dt,
    smi.sld_menu_itm_end_dt,
    smi.menu_itm_lfcy_ds,
    sihn_lvl1.itm_hrcy_node_id AS std_itm_lvl1_cd,
    sihn_lvl1.itm_hrcy_node_na AS std_itm_lvl1_na,
    sihn_lvl2.itm_hrcy_node_id AS std_itm_lvl2_cd,
    sihn_lvl2.itm_hrcy_node_na AS std_itm_lvl2_na,
    sihn_lvl3.itm_hrcy_node_id AS std_itm_lvl3_cd,
    sihn_lvl3.itm_hrcy_node_na AS std_itm_lvl3_na,
    sihn_lvl4.itm_hrcy_node_id AS std_itm_lvl4_cd,
    sihn_lvl4.itm_hrcy_node_na AS std_itm_lvl4_na,

    -- Pivoted characteristics (using Redshift syntax for COALESCE/MAX/SUM/CAST)
    COALESCE(MAX(CASE WHEN char_val.itm_char_id = 10000 THEN vld_list.itm_char_vld_list_ds END), '') AS menu_itm_fmly_grp_pren_ds,
    COALESCE(MAX(CASE WHEN char_val.itm_char_id = 10001 AND char_val.itm_char_gnrc_yes_no_val_fl = 1 THEN 'Y' ELSE 'N' END), '') AS gbal_core_prd_ds,
    COALESCE(MAX(CASE WHEN char_val.itm_char_id = 10002 THEN vld_list.itm_char_vld_list_ds END), '') AS menu_itm_regn_ds,
    COALESCE(MAX(CASE WHEN char_val.itm_char_id = 10009 THEN vld_list.itm_char_vld_list_ds END), '') AS dcps_cat_ds,
    COALESCE(MAX(CASE WHEN char_val.itm_char_id = 10010 THEN vld_list.itm_char_vld_list_ds END), '') AS menu_itm_strc_inhr_ds,
    COALESCE(MAX(CASE WHEN char_val.itm_char_id = 10011 THEN vld_list.itm_char_vld_list_ds END), '') AS menu_itm_fmly_grp_chld_ds,
    COALESCE(MAX(CASE WHEN char_val.itm_char_id = 10012 THEN vld_list.itm_char_vld_list_ds END), '') AS menu_itm_strc_non_inhr_ds,
    COALESCE(MAX(CASE WHEN char_val.itm_char_id = 10013 THEN vld_list.itm_char_vld_list_ds END), '') AS menu_itm_pri_prot_typ_ds,
    COALESCE(MAX(CASE WHEN char_val.itm_char_id = 10014 THEN vld_list.itm_char_vld_list_ds END), '') AS menu_itm_pri_prot_dtl_ds,
    COALESCE(MAX(CASE WHEN char_val.itm_char_id = 10015 THEN vld_list.itm_char_vld_list_ds END), '') AS menu_itm_scdy_prot_typ_ds,
    COALESCE(MAX(CASE WHEN char_val.itm_char_id = 10016 THEN vld_list.itm_char_vld_list_ds END), '') AS menu_itm_scdy_prot_dtl_ds,
    COALESCE(MAX(CASE WHEN char_val.itm_char_id = 10017 THEN vld_list.itm_char_vld_list_ds END), '') AS beef_ptty_size_ds,
    SUM(CASE WHEN char_val.itm_char_id = 10018 THEN char_val.itm_char_val_nu END) AS pri_prot_ptty_cnt_qt,
    COALESCE(MAX(CASE WHEN char_val.itm_char_id = 10019 THEN vld_list.itm_char_vld_list_ds END), '') AS veg_vegan_itm_ds,
    COALESCE(MAX(CASE WHEN char_val.itm_char_id = 10024 THEN vld_list.itm_char_vld_list_ds END), '') AS fngr_food_ds,
    COALESCE(MAX(CASE WHEN char_val.itm_char_id = 10025 THEN vld_list.itm_char_vld_list_ds END), '') AS menu_itm_prep_meth_ds,
    COALESCE(MAX(CASE WHEN char_val.itm_char_id = 10026 THEN vld_list.itm_char_vld_list_ds END), '') AS pri_prep_eqpm_ds,
    COALESCE(MAX(CASE WHEN char_val.itm_char_id = 10027 THEN vld_list.itm_char_vld_list_ds END), '') AS scdy_prep_eqpm_ds,
    COALESCE(MAX(CASE WHEN char_val.itm_char_id = 10028 THEN vld_list.itm_char_vld_list_ds END), '') AS menu_itm_prep_lcat_ds,
    COALESCE(MAX(CASE WHEN char_val.itm_char_id = 10029 THEN vld_list.itm_char_vld_list_ds END), '') AS pri_menu_itm_srvg_tmpr_ds,
    COALESCE(MAX(CASE WHEN char_val.itm_char_id = 10030 THEN vld_list.itm_char_vld_list_ds END), '') AS menu_itm_bred_form_ds,
    COALESCE(MAX(CASE WHEN char_val.itm_char_id = 10031 THEN vld_list.itm_char_vld_list_ds END), '') AS prep_meth_bred_ds,
    COALESCE(MAX(CASE WHEN char_val.itm_char_id = 10032 THEN vld_list.itm_char_vld_list_ds END), '') AS menu_itm_bred_typ_ds,
    SUM(CASE WHEN char_val.itm_char_id = 10035 THEN char_val.itm_char_val_nu END) AS bev_fluid_size_ds,
    COALESCE(MAX(CASE WHEN char_val.itm_char_id = 10036 THEN vld_list.itm_char_vld_list_ds END), '') AS bev_fluid_uom_ds,
    COALESCE(MAX(CASE WHEN char_val.itm_char_id = 10037 THEN vld_list.itm_char_vld_list_ds END), '') AS std_menu_itm_size_ds,
    COALESCE(MAX(CASE WHEN char_val.itm_char_id = 10053 THEN vld_list.itm_char_vld_list_ds END), '') AS mkt_menu_itm_size_ds,
    COALESCE(MAX(CASE WHEN char_val.itm_char_id = 10038 THEN vld_list.itm_char_vld_list_ds END), '') AS bev_form_ds,
    COALESCE(MAX(CASE WHEN char_val.itm_char_id = 10039 THEN vld_list.itm_char_vld_list_ds END), '') AS bev_caff_ds,
    COALESCE(MAX(CASE WHEN char_val.itm_char_id = 10040 THEN vld_list.itm_char_vld_list_ds END), '') AS tea_typ_ds,
    COALESCE(MAX(CASE WHEN char_val.itm_char_id = 10041 THEN vld_list.itm_char_vld_list_ds END), '') AS bev_crbn_ds,
    COALESCE(MAX(CASE WHEN char_val.itm_char_id = 10042 THEN vld_list.itm_char_vld_list_ds END), '') AS bev_swee_ds,
    COALESCE(MAX(CASE WHEN char_val.itm_char_id = 10043 THEN vld_list.itm_char_vld_list_ds END), '') AS bev_milk_typ_ds,
    SUM(CASE WHEN char_val.itm_char_id = 10048 THEN char_val.itm_char_val_nu END) AS menu_itm_cnt_qt,
    SUM(CASE WHEN char_val.itm_char_id = 10049 THEN char_val.itm_char_val_nu END) AS menu_itm_mult_cnt_qt,
    COALESCE(MAX(CASE WHEN char_val.itm_char_id = 10050 THEN vld_list.itm_char_vld_list_ds END), '') AS menu_itm_rvnu_stus_resn_ds,
    COALESCE(MAX(CASE WHEN char_val.itm_char_id = 10052 THEN char_val.itm_char_gnrc_yes_no_val_fl END), NULL) AS mcaf_prd_fl,
    COALESCE(MAX(CASE WHEN char_val.itm_char_id = 10003 THEN char_val.itm_char_val_ds END), '') AS menu_itm_purp_ds,
    COALESCE(MAX(CASE WHEN char_val.itm_char_id = 10033 THEN char_val.itm_char_val_ds END), '') AS menu_itm_topg_ds,
    COALESCE(MAX(CASE WHEN char_val.itm_char_id = 10034 THEN char_val.itm_char_val_ds END), '') AS menu_itm_topg_dtl_ds,
    COALESCE(MAX(CASE WHEN char_val.itm_char_id = 10044 THEN char_val.itm_char_val_ds END), '') AS menu_itm_flav_ds,
    COALESCE(MAX(CASE WHEN char_val.itm_char_id = 10045 THEN char_val.itm_char_val_ds END), '') AS menu_itm_flav_cat_ds,
    COALESCE(MAX(CASE WHEN char_val.itm_char_id = 10051 THEN char_val.itm_char_val_ds END), '') AS menu_itm_flav_aplc_meth_ds,
    COALESCE(MAX(CASE WHEN char_val.itm_char_id = 10046 THEN vld_list.itm_char_vld_list_ds END), '') AS menu_itm_brnd_ds,
    COALESCE(MAX(CASE WHEN char_val.itm_char_id = 10047 THEN char_val.itm_char_val_ds END), '') AS menu_itm_pkg_ds
FROM
    {{ source('Redshift_gdap_Schema', 'std_sld_menu_itm') }} AS smi
JOIN
    {{ source('Redshift_gdap_Schema', 'gbal_menu_itm') }} AS gmi
    ON smi.gbal_menu_itm_id = gmi.gbal_menu_itm_id
JOIN
    {{ source('Redshift_gdap_Schema', 'std_itm_hrcy_node_asgn') }} AS na_lvl4
    ON gmi.itm_hrcy_node_id = na_lvl4.itm_hrcy_node_id
JOIN
    {{ source('Redshift_gdap_Schema', 'std_itm_hrcy_node') }} AS sihn_lvl4
    ON na_lvl4.itm_hrcy_node_id = sihn_lvl4.itm_hrcy_node_id
JOIN
    {{ source('Redshift_gdap_Schema', 'std_itm_hrcy_node_asgn') }} AS na_lvl3
    ON na_lvl3.itm_hrcy_node_id = na_lvl4.pren_itm_hrcy_node_id
JOIN
    {{ source('Redshift_gdap_Schema', 'std_itm_hrcy_node') }} AS sihn_lvl3
    ON na_lvl3.itm_hrcy_node_id = sihn_lvl3.itm_hrcy_node_id
JOIN
    {{ source('Redshift_gdap_Schema', 'std_itm_hrcy_node_asgn') }} AS na_lvl2
    ON na_lvl2.itm_hrcy_node_id = na_lvl3.pren_itm_hrcy_node_id
JOIN
    {{ source('Redshift_gdap_Schema', 'std_itm_hrcy_node') }} AS sihn_lvl2
    ON na_lvl2.itm_hrcy_node_id = sihn_lvl2.itm_hrcy_node_id
JOIN
    {{ source('Redshift_gdap_Schema', 'std_itm_hrcy_node_asgn') }} AS na_lvl1
    ON na_lvl1.itm_hrcy_node_id = na_lvl2.pren_itm_hrcy_node_id
JOIN
    {{ source('Redshift_gdap_Schema', 'std_itm_hrcy_node') }} AS sihn_lvl1
    ON na_lvl1.itm_hrcy_node_id = sihn_lvl1.itm_hrcy_node_id AND na_lvl1.pren_itm_hrcy_node_id = 0
LEFT JOIN
    {{ source('Redshift_gdap_Schema', 'std_itm_char_val') }} AS char_val
    ON smi.sld_menu_itm_id = char_val.sld_menu_itm_id
    AND smi.terr_cd = char_val.terr_cd
    AND char_val.itm_char_end_dt IS NULL
LEFT JOIN
    {{ source('Redshift_gdap_Schema', 'std_itm_char_vld_list') }} AS vld_list
    ON vld_list.itm_char_id = char_val.itm_char_id
    AND char_val.itm_char_vld_list_id = vld_list.itm_char_vld_list_id
GROUP BY
    smi.terr_cd, smi.sld_menu_itm_id, smi.sld_menu_itm_na, smi.sld_menu_itm_shrt_na, smi.sld_menu_itm_ds, smi.sld_menu_itm_cert_typ_na,
    smi.gbal_menu_itm_id, gmi.gbal_menu_itm_na, smi.sld_menu_itm_stus_cd, smi.sld_menu_itm_intd_dt, smi.sld_menu_itm_end_dt, smi.menu_itm_lfcy_ds,
    sihn_lvl1.itm_hrcy_node_id, sihn_lvl1.itm_hrcy_node_na, sihn_lvl2.itm_hrcy_node_id, sihn_lvl2.itm_hrcy_node_na, sihn_lvl3.itm_hrcy_node_id,
    sihn_lvl3.itm_hrcy_node_na, sihn_lvl4.itm_hrcy_node_id, sihn_lvl4.itm_hrcy_node_na