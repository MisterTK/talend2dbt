{% macro redshift_delete_and_insert_1(target_relation, source_relation, join_key_columns) %}

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

        -- INSERT new data
        INSERT INTO {{ target_relation }} (
            itm_hrcy_node_id,
            hrcy_node_asgn_eff_dt,
            pren_itm_hrcy_node_id,
            hrcy_node_asgn_end_dt,
            load_dw_audt_ts,
            updt_dw_audt_ts,
            dw_file_id
        )
        SELECT
            itm_hrcy_node_id,
            hrcy_node_asgn_eff_dt,
            pren_itm_hrcy_node_id,
            hrcy_node_asgn_end_dt,
            CURRENT_DATETIME as load_dw_audt_ts,
            CURRENT_DATETIME as updt_dw_audt_ts,
            dw_file_id
        FROM {{ source_relation }};
    {% endif %}

{% endmacro %}
