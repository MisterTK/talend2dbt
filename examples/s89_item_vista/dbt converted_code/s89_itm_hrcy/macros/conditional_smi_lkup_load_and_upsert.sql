-- macros/conditional_smi_lkup_load_and_upsert.sql

{% macro conditional_smi_lkup_load_and_upsert(target_relation, source_model_ref, stage_smi_source_ref) %}

    {# 1. Get count of the target table (std_sld_menu_itm_lkup) #}
    {% set smi_lkp_count = -1 %} -- Initialize with a sentinel value

    {% if execute %}
        {# Check if the target_relation actually exists before querying it #}
        {% set target_exists = adapter.get_relation(target_relation.database, target_relation.schema, target_relation.name) %}

        {% if target_exists %}
            {% set target_table_count_query = "SELECT count(1) FROM " ~ target_relation %}
            {% set results = run_query(target_table_count_query) %}
            {% set smi_lkp_count = results.columns[0].values()[0] %}
            {{ log("Current count of " ~ target_relation ~ ": " ~ smi_lkp_count, info=true) }}
        {% else %}
            {# If the target table doesn't exist yet, it's effectively empty #}
            {% set smi_lkp_count = 0 %}
            {{ log("Target table " ~ target_relation ~ " does not exist yet. Assuming count is 0 for initial load logic.", info=true) }}
        {% endif %}
    {% endif %}


    {% if smi_lkp_count == 0 %}
        -- Talend IF branch: Target table is empty, perform a full insert.
        {{ log("Target table is empty. Performing full insert into " ~ target_relation ~ ".", info=true) }}

        INSERT INTO {{ target_relation }} (
            -- ... (All your columns) ...
            terr_cd, sld_menu_itm_id, gbal_menu_itm_id, gbal_menu_itm_na, sld_menu_itm_na, sld_menu_itm_shrt_na, sld_menu_itm_ds,
            sld_menu_itm_cert_typ_na, sld_menu_itm_stus_cd, sld_menu_itm_intd_dt, sld_menu_itm_end_dt, menu_itm_lfcy_ds,
            std_itm_lvl1_cd, std_itm_lvl1_na, std_itm_lvl2_cd, std_itm_lvl2_na, std_itm_lvl3_cd, std_itm_lvl3_na,
            std_itm_lvl4_cd, std_itm_lvl4_na, menu_itm_fmly_grp_pren_ds, gbal_core_prd_ds, menu_itm_regn_ds,
            dcps_cat_ds, menu_itm_strc_inhr_ds, menu_itm_fmly_grp_chld_ds, menu_itm_strc_non_inhr_ds,
            menu_itm_pri_prot_typ_ds, menu_itm_pri_prot_dtl_ds, menu_itm_pri_prot_dtl_ds, menu_itm_scdy_prot_typ_ds, menu_itm_scdy_prot_dtl_ds,
            beef_ptty_size_ds, pri_prot_ptty_cnt_qt, veg_vegan_itm_ds, fngr_food_ds, menu_itm_prep_meth_ds,
            pri_prep_eqpm_ds, scdy_prep_eqpm_ds, menu_itm_prep_lcat_ds, pri_menu_itm_srvg_tmpr_ds, menu_itm_bred_form_ds,
            prep_meth_bred_ds, menu_itm_bred_typ_ds, bev_fluid_size_ds, bev_fluid_uom_ds, std_menu_itm_size_ds,
            mkt_menu_itm_size_ds, bev_form_ds, bev_caff_ds, tea_typ_ds, bev_crbn_ds, bev_swee_ds, bev_milk_typ_ds,
            menu_itm_cnt_qt, menu_itm_mult_cnt_qt, menu_itm_rvnu_stus_resn_ds, mcaf_prd_fl, menu_itm_purp_ds,
            menu_itm_topg_ds, menu_itm_topg_dtl_ds, menu_itm_flav_ds, menu_itm_flav_cat_ds, menu_itm_flav_aplc_meth_ds,
            menu_itm_brnd_ds, menu_itm_pkg_ds
        )
        SELECT
            -- Select all columns from your base model (int_std_sld_menu_itm_lkup_base)
            terr_cd, sld_menu_itm_id, gbal_menu_itm_id, gbal_menu_itm_na, sld_menu_itm_na, sld_menu_itm_shrt_na, sld_menu_itm_ds,
            sld_menu_itm_cert_typ_na, sld_menu_itm_stus_cd, sld_menu_itm_intd_dt, sld_menu_itm_end_dt, menu_itm_lfcy_ds,
            std_itm_lvl1_cd, std_itm_lvl1_na, std_itm_lvl2_cd, std_itm_lvl2_na, std_itm_lvl3_cd, std_itm_lvl3_na,
            std_itm_lvl4_cd, std_itm_lvl4_na, CASE WHEN TRIM(menu_itm_fmly_grp_pren_ds) = '' THEN 'N/A' ELSE menu_itm_fmly_grp_pren_ds END,
            gbal_core_prd_ds, CASE WHEN TRIM(menu_itm_regn_ds) = '' THEN 'N/A' ELSE menu_itm_regn_ds END,
            CASE WHEN TRIM(dcps_cat_ds) = '' THEN 'N/A' ELSE dcps_cat_ds END,
            CASE WHEN TRIM(menu_itm_strc_inhr_ds) = '' THEN 'N/A' ELSE menu_itm_strc_inhr_ds END,
            CASE WHEN TRIM(menu_itm_fmly_grp_chld_ds) = '' THEN 'N/A' ELSE menu_itm_fmly_grp_chld_ds END,
            CASE WHEN TRIM(menu_itm_strc_non_inhr_ds) = '' THEN 'N/A' ELSE menu_itm_strc_non_inhr_ds END,
            CASE WHEN TRIM(menu_itm_pri_prot_typ_ds) = '' THEN 'N/A' ELSE menu_itm_pri_prot_typ_ds END,
            CASE WHEN TRIM(menu_itm_pri_prot_dtl_ds) = '' THEN 'N/A' ELSE menu_itm_pri_prot_dtl_ds END,
            CASE WHEN TRIM(menu_itm_scdy_prot_typ_ds) = '' THEN 'N/A' ELSE menu_itm_scdy_prot_typ_ds END,
            CASE WHEN TRIM(menu_itm_scdy_prot_dtl_ds) = '' THEN 'N/A' ELSE menu_itm_scdy_prot_dtl_ds END,
            CASE WHEN TRIM(beef_ptty_size_ds) = '' THEN 'N/A' ELSE beef_ptty_size_ds END,
            pri_prot_ptty_cnt_qt, CASE WHEN TRIM(veg_vegan_itm_ds) = '' THEN 'N/A' ELSE veg_vegan_itm_ds END,
            CASE WHEN TRIM(fngr_food_ds) = '' THEN 'N/A' ELSE fngr_food_ds END,
            CASE WHEN TRIM(menu_itm_prep_meth_ds) = '' THEN 'N/A' ELSE menu_itm_prep_meth_ds END,
            CASE WHEN TRIM(pri_prep_eqpm_ds) = '' THEN 'N/A' ELSE pri_prep_eqpm_ds END,
            CASE WHEN TRIM(scdy_prep_eqpm_ds) = '' THEN 'N/A' ELSE scdy_prep_eqpm_ds END,
            CASE WHEN TRIM(menu_itm_prep_lcat_ds) = '' THEN 'N/A' ELSE menu_itm_prep_lcat_ds END,
            CASE WHEN TRIM(pri_menu_itm_srvg_tmpr_ds) = '' THEN 'N/A' ELSE pri_menu_itm_srvg_tmpr_ds END,
            CASE WHEN TRIM(menu_itm_bred_form_ds) = '' THEN 'N/A' ELSE menu_itm_bred_form_ds END,
            CASE WHEN TRIM(prep_meth_bred_ds) = '' THEN 'N/A' ELSE prep_meth_bred_ds END,
            CASE WHEN TRIM(menu_itm_bred_typ_ds) = '' THEN 'N/A' ELSE menu_itm_bred_typ_ds END,
            bev_fluid_size_ds, CASE WHEN TRIM(bev_fluid_uom_ds) = '' THEN 'N/A' ELSE bev_fluid_uom_ds END,
            CASE WHEN TRIM(std_menu_itm_size_ds) = '' THEN 'N/A' ELSE std_menu_itm_size_ds END,
            CASE WHEN TRIM(mkt_menu_itm_size_ds) = '' THEN 'N/A' ELSE mkt_menu_itm_size_ds END,
            CASE WHEN TRIM(bev_form_ds) = '' THEN 'N/A' ELSE bev_form_ds END,
            CASE WHEN TRIM(bev_caff_ds) = '' THEN 'N/A' ELSE bev_caff_ds END,
            CASE WHEN TRIM(tea_typ_ds) = '' THEN 'N/A' ELSE tea_typ_ds END,
            CASE WHEN TRIM(bev_crbn_ds) = '' THEN 'N/A' ELSE bev_crbn_ds END,
            CASE WHEN TRIM(bev_swee_ds) = '' THEN 'N/A' ELSE bev_swee_ds END,
            CASE WHEN TRIM(bev_milk_typ_ds) = '' THEN 'N/A' ELSE bev_milk_typ_ds END,
            menu_itm_cnt_qt, menu_itm_mult_cnt_qt,
            CASE WHEN TRIM(menu_itm_rvnu_stus_resn_ds) = '' THEN 'N/A' ELSE menu_itm_rvnu_stus_resn_ds END,
            mcaf_prd_fl,
            CASE WHEN TRIM(menu_itm_purp_ds) = '' THEN 'N/A' ELSE menu_itm_purp_ds END,
            CASE WHEN TRIM(menu_itm_topg_ds) = '' THEN 'N/A' ELSE menu_itm_topg_ds END,
            CASE WHEN TRIM(menu_itm_topg_dtl_ds) = '' THEN 'N/A' ELSE menu_itm_topg_dtl_ds END,
            CASE WHEN TRIM(menu_itm_flav_ds) = '' THEN 'N/A' ELSE menu_itm_flav_ds END,
            CASE WHEN TRIM(menu_itm_flav_cat_ds) = '' THEN 'N/A' ELSE menu_itm_flav_cat_ds END,
            CASE WHEN TRIM(menu_itm_flav_aplc_meth_ds) = '' THEN 'N/A' ELSE menu_itm_flav_aplc_meth_ds END,
            CASE WHEN TRIM(menu_itm_brnd_ds) = '' THEN 'N/A' ELSE menu_itm_brnd_ds END,
            CASE WHEN TRIM(menu_itm_pkg_ds) = '' THEN 'N/A' ELSE menu_itm_pkg_ds END
        FROM
            {{ source_model_ref }}
        ;

    {% else %}
        -- Talend ELSE branch: Target table is NOT empty.
        {# 2. Get count of the stage source table (std_sld_menu_itm) #}
        {% set stage_smi_count_query = "SELECT count(1) FROM " ~ stage_smi_source_ref %}
        {% set stage_results = run_query(stage_smi_count_query) %}
        {% set smi_val_count = stage_results.columns[0].values()[0] %}

        {{ log("Target table " ~ target_relation ~ " is not empty. Stage source (" ~ stage_smi_source_ref ~ ") count is: " ~ smi_val_count, info=true) }}

        {% if smi_val_count > 0 %}
            -- Talend nested IF branch: Stage source has data, perform DELETE + INSERT (Upsert)
            {{ log("Stage source has data. Performing DELETE + INSERT (upsert) into " ~ target_relation ~ ".", info=true) }}

            DELETE FROM {{ target_relation }}
            USING {{ stage_smi_source_ref }} -- Using the stage table for matching
            WHERE
                {{ target_relation }}.TERR_CD = {{ stage_smi_source_ref }}.TERR_CD
                AND {{ target_relation }}.SLD_MENU_ITM_ID = {{ stage_smi_source_ref }}.SLD_MENU_ITM_ID
            ;

            INSERT INTO {{ target_relation }} (
                -- List all target columns again, must match the SELECT order/names
                terr_cd, sld_menu_itm_id, gbal_menu_itm_id, gbal_menu_itm_na, sld_menu_itm_na, sld_menu_itm_shrt_na, sld_menu_itm_ds,
                sld_menu_itm_cert_typ_na, sld_menu_itm_stus_cd, sld_menu_itm_intd_dt, sld_menu_itm_end_dt, menu_itm_lfcy_ds,
                std_itm_lvl1_cd, std_itm_lvl1_na, std_itm_lvl2_cd, std_itm_lvl2_na, std_itm_lvl3_cd, std_itm_lvl3_na,
                std_itm_lvl4_cd, std_itm_lvl4_na, menu_itm_fmly_grp_pren_ds, gbal_core_prd_ds, menu_itm_regn_ds,
                dcps_cat_ds, menu_itm_strc_inhr_ds, menu_itm_fmly_grp_chld_ds, menu_itm_strc_non_inhr_ds,
                menu_itm_pri_prot_typ_ds, menu_itm_pri_prot_dtl_ds, menu_itm_scdy_prot_typ_ds, menu_itm_scdy_prot_dtl_ds,
                beef_ptty_size_ds, pri_prot_ptty_cnt_qt, veg_vegan_itm_ds, fngr_food_ds, menu_itm_prep_meth_ds,
                pri_prep_eqpm_ds, scdy_prep_eqpm_ds, menu_itm_prep_lcat_ds, pri_menu_itm_srvg_tmpr_ds, menu_itm_bred_form_ds,
                prep_meth_bred_ds, menu_itm_bred_typ_ds, bev_fluid_size_ds, bev_fluid_uom_ds, std_menu_itm_size_ds,
                mkt_menu_itm_size_ds, bev_form_ds, bev_caff_ds, tea_typ_ds, bev_crbn_ds, bev_swee_ds, bev_milk_typ_ds,
                menu_itm_cnt_qt, menu_itm_mult_cnt_qt, menu_itm_rvnu_stus_resn_ds, mcaf_prd_fl, menu_itm_purp_ds,
                menu_itm_topg_ds, menu_itm_topg_dtl_ds, menu_itm_flav_ds, menu_itm_flav_cat_ds, menu_itm_flav_aplc_meth_ds,
                menu_itm_brnd_ds, menu_itm_pkg_ds
            )
            SELECT
                -- Select all columns from your base model (int_std_sld_menu_itm_lkup_base)
                terr_cd, sld_menu_itm_id, gbal_menu_itm_id, gbal_menu_itm_na, sld_menu_itm_na, sld_menu_itm_shrt_na, sld_menu_itm_ds,
                sld_menu_itm_cert_typ_na, sld_menu_itm_stus_cd, sld_menu_itm_intd_dt, sld_menu_itm_end_dt, menu_itm_lfcy_ds,
                std_itm_lvl1_cd, std_itm_lvl1_na, std_itm_lvl2_cd, std_itm_lvl2_na, std_itm_lvl3_cd, std_itm_lvl3_na,
                std_itm_lvl4_cd, std_itm_lvl4_na, CASE WHEN TRIM(menu_itm_fmly_grp_pren_ds) = '' THEN 'N/A' ELSE menu_itm_fmly_grp_pren_ds END,
                gbal_core_prd_ds, CASE WHEN TRIM(menu_itm_regn_ds) = '' THEN 'N/A' ELSE menu_itm_regn_ds END,
                CASE WHEN TRIM(dcps_cat_ds) = '' THEN 'N/A' ELSE dcps_cat_ds END,
                CASE WHEN TRIM(menu_itm_strc_inhr_ds) = '' THEN 'N/A' ELSE menu_itm_strc_inhr_ds END,
                CASE WHEN TRIM(menu_itm_fmly_grp_chld_ds) = '' THEN 'N/A' ELSE menu_itm_fmly_grp_chld_ds END,
                CASE WHEN TRIM(menu_itm_strc_non_inhr_ds) = '' THEN 'N/A' ELSE menu_itm_strc_non_inhr_ds END,
                CASE WHEN TRIM(menu_itm_pri_prot_typ_ds) = '' THEN 'N/A' ELSE menu_itm_pri_prot_typ_ds END,
                CASE WHEN TRIM(menu_itm_pri_prot_dtl_ds) = '' THEN 'N/A' ELSE menu_itm_pri_prot_dtl_ds END,
                CASE WHEN TRIM(menu_itm_scdy_prot_typ_ds) = '' THEN 'N/A' ELSE menu_itm_scdy_prot_typ_ds END,
                CASE WHEN TRIM(menu_itm_scdy_prot_dtl_ds) = '' THEN 'N/A' ELSE menu_itm_scdy_prot_dtl_ds END,
                CASE WHEN TRIM(beef_ptty_size_ds) = '' THEN 'N/A' ELSE beef_ptty_size_ds END,
                pri_prot_ptty_cnt_qt, CASE WHEN TRIM(veg_vegan_itm_ds) = '' THEN 'N/A' ELSE veg_vegan_itm_ds END,
                CASE WHEN TRIM(fngr_food_ds) = '' THEN 'N/A' ELSE fngr_food_ds END,
                CASE WHEN TRIM(menu_itm_prep_meth_ds) = '' THEN 'N/A' ELSE menu_itm_prep_meth_ds END,
                CASE WHEN TRIM(pri_prep_eqpm_ds) = '' THEN 'N/A' ELSE pri_prep_eqpm_ds END,
                CASE WHEN TRIM(scdy_prep_eqpm_ds) = '' THEN 'N/A' ELSE scdy_prep_eqpm_ds END,
                CASE WHEN TRIM(menu_itm_prep_lcat_ds) = '' THEN 'N/A' ELSE menu_itm_prep_lcat_ds END,
                CASE WHEN TRIM(pri_menu_itm_srvg_tmpr_ds) = '' THEN 'N/A' ELSE pri_menu_itm_srvg_tmpr_ds END,
                CASE WHEN TRIM(menu_itm_bred_form_ds) = '' THEN 'N/A' ELSE menu_itm_bred_form_ds END,
                CASE WHEN TRIM(prep_meth_bred_ds) = '' THEN 'N/A' ELSE prep_meth_bred_ds END,
                CASE WHEN TRIM(menu_itm_bred_typ_ds) = '' THEN 'N/A' ELSE menu_itm_bred_typ_ds END,
                bev_fluid_size_ds, CASE WHEN TRIM(bev_fluid_uom_ds) = '' THEN 'N/A' ELSE bev_fluid_uom_ds END,
                CASE WHEN TRIM(std_menu_itm_size_ds) = '' THEN 'N/A' ELSE std_menu_itm_size_ds END,
                CASE WHEN TRIM(mkt_menu_itm_size_ds) = '' THEN 'N/A' ELSE mkt_menu_itm_size_ds END,
                CASE WHEN TRIM(bev_form_ds) = '' THEN 'N/A' ELSE bev_form_ds END,
                CASE WHEN TRIM(bev_caff_ds) = '' THEN 'N/A' ELSE bev_caff_ds END,
                CASE WHEN TRIM(tea_typ_ds) = '' THEN 'N/A' ELSE tea_typ_ds END,
                CASE WHEN TRIM(bev_crbn_ds) = '' THEN 'N/A' ELSE bev_crbn_ds END,
                CASE WHEN TRIM(bev_swee_ds) = '' THEN 'N/A' ELSE bev_swee_ds END,
                CASE WHEN TRIM(bev_milk_typ_ds) = '' THEN 'N/A' ELSE bev_milk_typ_ds END,
                menu_itm_cnt_qt, menu_itm_mult_cnt_qt,
                CASE WHEN TRIM(menu_itm_rvnu_stus_resn_ds) = '' THEN 'N/A' ELSE menu_itm_rvnu_stus_resn_ds END,
                mcaf_prd_fl,
                CASE WHEN TRIM(menu_itm_purp_ds) = '' THEN 'N/A' ELSE menu_itm_purp_ds END,
                CASE WHEN TRIM(menu_itm_topg_ds) = '' THEN 'N/A' ELSE menu_itm_topg_ds END,
                CASE WHEN TRIM(menu_itm_topg_dtl_ds) = '' THEN 'N/A' ELSE menu_itm_topg_dtl_ds END,
                CASE WHEN TRIM(menu_itm_flav_ds) = '' THEN 'N/A' ELSE menu_itm_flav_ds END,
                CASE WHEN TRIM(menu_itm_flav_cat_ds) = '' THEN 'N/A' ELSE menu_itm_flav_cat_ds END,
                CASE WHEN TRIM(menu_itm_flav_aplc_meth_ds) = '' THEN 'N/A' ELSE menu_itm_flav_aplc_meth_ds END,
                CASE WHEN TRIM(menu_itm_brnd_ds) = '' THEN 'N/A' ELSE menu_itm_brnd_ds END,
                CASE WHEN TRIM(menu_itm_pkg_ds) = '' THEN 'N/A' ELSE menu_itm_pkg_ds END
            FROM
                {{ source_model_ref }}
            ;
        {% else %}
            -- Talend nested ELSE branch: Stage source has NO data, so do nothing to the target table.
            {{ log("Stage source (" ~ stage_smi_source_ref ~ ") is empty. Skipping update/merge for " ~ target_relation ~ ".", info=true) }}
        {% endif %}

    {% endif %}

{% endmacro %}