{% macro redshift_delete_and_insert_2(target_relation, source_relation, join_key_columns) %}

    {% set join_conditions = [] %}
    {% for col in join_key_columns %}
        {% do join_conditions.append("target." ~ col ~ " = source." ~ col) %}
    {% endfor %}

    {% if execute %}
        -- DELETE using EXISTS instead of USING
        DELETE FROM {{ target_relation }} AS target
        WHERE EXISTS (
            SELECT 1
            FROM {{ source_relation }} AS source
            WHERE {{ join_conditions | join(' AND ') }}
        );

        INSERT INTO {{ target_relation }} (
            -- List all columns from the target table to be inserted
            itm_hrcy_lvl_nu,
            itm_hrcy_lvl_eff_dt,
            lvl_node_asgn_eff_dt,
            itm_hrcy_node_id,
            lvl_node_asgn_end_dt,
            load_dw_audt_ts,
            updt_dw_audt_ts,
            dw_file_id
        )
        SELECT
            -- List all columns from the source table to be selected
            itm_hrcy_lvl_nu,
            itm_hrcy_lvl_eff_dt,
            lvl_node_asgn_eff_dt,
            itm_hrcy_node_id,
            lvl_node_asgn_end_dt,
            CURRENT_DATETIME as load_dw_audt_ts,
            CURRENT_DATETIME as updt_dw_audt_ts,
            dw_file_id
        FROM {{ source_relation }};
    {% endif %}
{% endmacro %}