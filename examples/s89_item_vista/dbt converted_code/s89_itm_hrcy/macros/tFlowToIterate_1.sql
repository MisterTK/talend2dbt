-- macros/tFlowToIterate_1.sql
-- This macro orchestrates dynamic loads, simulating tFlowToIterate.
-- It iterates through a source model's rows to dynamically execute SQL for each row.

{% macro tFlowToIterate_1(source_model_for_iteration, target_schema) %} {# base_gcs_path removed #}

  -- 1. Get the data to iterate over (simulates tFlowToIterate's input 'row3')
  {% set sql_to_iterate = "SELECT TERR_CD, SLD_MENU_ITM_ID FROM " ~ ref(source_model_for_iteration) %}
  {% set results = run_query(sql_to_iterate) %}

  {% if execute %}
    {% for row in results %}
      {% set current_terr_cd = row.TERR_CD %}
      {% set current_smi_id = row.SLD_MENU_ITM_ID %}

      -- Example of what tDBInput_1 (tRedshiftInput) might do for each iteration:
      -- It would run a query like:
      -- SELECT terr_cd, sld_menu_itm_id, ... FROM std_itm_char_val WHERE terr_cd = current_terr_cd AND sld_menu_itm_id = current_smi_id;
      -- And then this result might feed filtered_itm_char_val.
      --
      -- In dbt, if this is purely for filtering, it's done via a set-based JOIN in the model itself.
      -- If it's for DML (e.g., creating a table for each row), then dynamic DDL/DML via execute immediate.

      -- This macro itself will not return data, but execute DML/DDL.
      -- So, there will be no direct 'output' from this macro that's a SELECT statement.
      -- It's primarily for orchestration.

      {#
      -- Example: Execute a DML or DDL for each iteration.
      -- Replace with the actual DML/DDL that the tRedshiftInput + tFixedFlowInput + tFileOutputDelimited / other components downstream of tFlowToIterate would execute.
      {% set dynamic_dml %}
        INSERT INTO `{{ target.database }}`.`{{ target_schema }}`.`dynamic_output_table_for_{{ current_terr_cd }}_{{ current_smi_id }}` (
          terr_cd, sld_menu_itm_id, itm_char_id, ...
        )
        SELECT
          terr_cd, sld_menu_itm_id, itm_char_id, ...
        FROM {{ source('rmdw_tables', 'std_itm_char_val') }} -- Or another source/ref
        WHERE terr_cd = {{ current_terr_cd }} AND sld_menu_itm_id = {{ current_smi_id }}
      {% endset %}

      {{ log("Running dynamic DML for TERR_CD=" ~ current_terr_cd ~ ", SMI_ID=" ~ current_smi_id, info=True) }}
      {{ run_query(dynamic_dml) }}
      #}

    {% endfor %}
  {% endif %}

{% endmacro %}