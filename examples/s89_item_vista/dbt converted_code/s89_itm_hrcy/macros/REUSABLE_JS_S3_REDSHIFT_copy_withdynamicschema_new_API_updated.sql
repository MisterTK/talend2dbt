---

{% macro REUSABLE_JS_S3_REDSHIFT_copy_withdynamicschema_new_API_updated(target_dataset_name, target_table_name, source_dataset_name, source_table_name, is_delete=1) %}
    {#
    This macro is designed to copy data from a source table to a target table
    within the specified datasets, dynamically fetching column names and excluding
    audit columns. It performs DML operations (TRUNCATE and INSERT).

    Parameters:
    - target_dataset_name (string): The name of the BigQuery dataset for the target table.
    - target_table_name (string): The name of the target table to insert data into.
    - source_dataset_name (string): The name of the BigQuery dataset for the source table.
    - source_table_name (string): The name of the source table to select data from.
    - is_delete (integer): If 1, the target table will be truncated before insertion.
                           If 0, data will be inserted without truncation.

    Important: This macro performs database operations (run_query, adapter.execute)
    and therefore MUST be guarded by {% if execute %} to prevent compilation errors
    when dbt is in the parsing phase.
    #}

    {% set exclude_columns = ["load_dw_audt_ts", "updt_dw_audt_ts"] %}

    {% if execute %}
        {{ log("DEBUG: REUSABLE_JS_S3_REDSHIFT_copy_withdynamicschema_new_API is in EXECUTION phase for target: " ~ target_dataset_name ~ "." ~ target_table_name, info=true) }}

        -- Step 1: Fetch column names for the target table
        {% set target_cols_query %}
            SELECT column_name
            FROM {{ target_dataset_name }}.INFORMATION_SCHEMA.COLUMNS
            WHERE table_name = '{{ target_table_name }}'
            AND column_name NOT IN ({{ exclude_columns | map('tojson') | join(', ') }})
        {% endset %}

        {% set target_cols_result = run_query(target_cols_query) %}

        {% if target_cols_result is none or target_cols_result.columns | length == 0 %}
            {% do exceptions.raise_compiler_error("Error in REUSABLE_JS_S3_REDSHIFT_copy_withdynamicschema_new_API: Target columns query returned no result or no columns for " ~ target_dataset_name ~ "." ~ target_table_name) %}
        {% endif %}

        {% set target_cols = target_cols_result.columns[0].values() | join(', ') %}
        {{ log("DEBUG: Target columns fetched: " ~ target_cols, info=true) }}

        -- Step 2: Fetch column names for the source table
        {% set source_cols_query %}
            SELECT column_name
            FROM {{ source_dataset_name }}.INFORMATION_SCHEMA.COLUMNS
            WHERE table_name = '{{ source_table_name }}'
            AND column_name NOT IN ({{ exclude_columns | map('tojson') | join(', ') }})
        {% endset %}

        {% set source_cols_result = run_query(source_cols_query) %}

        {% if source_cols_result is none or source_cols_result.columns | length == 0 %}
            {% do exceptions.raise_compiler_error("Error in REUSABLE_JS_S3_REDSHIFT_copy_withdynamicschema_new_API: Source columns query returned no result or no columns for " ~ source_dataset_name ~ "." ~ source_table_name) %}
        {% endif %}

        {% set source_cols = source_cols_result.columns[0].values() | join(', ') %}
        {{ log("DEBUG: Source columns fetched: " ~ source_cols, info=true) }}

        ---
        -- Step 3: Optionally truncate the target table
        ---
        {% if is_delete == 1 %}
            {% set truncate_sql %}
                TRUNCATE TABLE {{ target_dataset_name }}.{{ target_table_name }};
            {% endset %}
            {{ log("Truncating table: " ~ target_dataset_name ~ "." ~ target_table_name, info=True) }}
            {% do adapter.execute(truncate_sql) %}
        {% endif %}

        ---
        -- Step 4: Build and execute the insert query
        ---
        {% set insert_sql %}
            INSERT INTO {{ target_dataset_name }}.{{ target_table_name }} ({{ target_cols }})
            SELECT {{ source_cols }}
            FROM {{ source_dataset_name }}.{{ source_table_name }};
        {% endset %}

        {{ log("Running insert: " ~ insert_sql, info=True) }}
        {% do adapter.execute(insert_sql) %}

    {% else %}
        {{ log("DEBUG: REUSABLE_JS_S3_REDSHIFT_copy_withdynamicschema_new_API is in COMPILATION phase. Skipping database operations.", info=true) }}
    {% endif %}
    select 1 as dummy_column
{% endmacro %}