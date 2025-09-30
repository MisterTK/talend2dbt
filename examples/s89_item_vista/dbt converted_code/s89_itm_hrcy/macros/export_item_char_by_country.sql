-- macros/export_item_char_by_country.sql
-- This macro replaces the tFlowToIterate + tFixedFlowInput pattern
-- when used to prepare data for dynamic operations like file exports.

{% macro export_item_char_by_country(source_model_for_iteration) %}

  -- 1. Get the data to iterate over (simulates the combined flow of tFlowToIterate and tFixedFlowInput's input)
  -- This query now retrieves ALL columns from the specified source model.
  {% set sql_to_iterate = "SELECT * FROM " ~ ref(source_model_for_iteration) %}
  {% set results = run_query(sql_to_iterate) %}

  {% if execute %}
    {% for row in results %}
      {# Access all values from the current row dynamically. #}
      {# This simulates tFixedFlowInput reading from globalMap for all columns. #}

      -- You would now access specific columns like:
      {% set current_terr_cd = row.TERR_CD %}
      {% set current_smi_id = row.SLD_MENU_ITM_ID %}
      {% set current_itm_char_id = row.ITM_CHAR_ID %}
      {% set current_itm_char_vld_list_id = row.ITM_CHAR_VLD_LIST_ID %}
      {% set current_itm_char_val_ds = row.ITM_CHAR_VAL_DS %}
      {% set current_itm_char_eff_dt = row.ITM_CHAR_EFF_DT %}
      {% set current_itm_char_end_dt = row.ITM_CHAR_END_DT %}
      {% set current_file_rec_ts = row.FILE_REC_TS %}
      {% set current_dw_file_id = row.DW_FILE_ID %} {# DW_FILE_ID is now included, as its from SELECT * #}
      {# Add other columns present in your source model if needed here #}


      -- 2. Placeholder for downstream dynamic operation (e.g., calling another macro or executing DML/DDL)
      {#
      -- If you are inserting/updating, you can dynamically build the column list and values.
      -- This is more robust than hardcoding every column.

      {% set column_names = adapter.get_columns_in_relation(ref(source_model_for_iteration)) %}
      {% set insert_columns = [] %}
      {% set insert_values = [] %}

      {% for column in column_names %}
        {% do insert_columns.append(column.name) %}
        {% set value = row[column.name] %}
        {% if column.data_type == 'STRING' or column.data_type == 'DATE' or column.data_type == 'TIMESTAMP' %}
          {% do insert_values.append("'" ~ value ~ "'") %}
        {% else %}
          {% do insert_values.append(value | string) %}
        {% endif %}
      {% endfor %}

      {% set dynamic_dml %}
        INSERT INTO `{{ target.database }}.{{ target.schema }}.your_per_row_table_output` (
          {{ insert_columns | join(', ') }}
        )
        VALUES (
          {{ insert_values | join(', ') }}
        )
      {% endset %}

      {{ log("Running dynamic DML for TERR_CD=" ~ current_terr_cd ~ ", SMI_ID=" ~ current_smi_id, info=True) }}
      {{ run_query(dynamic_dml) }}
      #}

    {% endfor %}
  {% endif %}

{% endmacro %}