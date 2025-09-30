{% macro REUSABLE_JS_S3_REDSHIFT_copy_withdynamicschema_new_API(dataset_name, table_name, source_table, is_delete=1) %}
    {#
    This macro is designed to copy data from a source table to a target table
    within the specified dataset, dynamically fetching column names and excluding
    audit columns. It performs DML operations (TRUNCATE and INSERT).

    Parameters:
    - dataset_name (string): The name of the BigQuery dataset where both tables reside.
    - table_name (string): The name of the target table to insert data into.
    - source_table (string): The name of the source table to select data from.
    - is_delete (integer): If 1, the target table will be truncated before insertion.
                           If 0, data will be inserted without truncation.

    Important: This macro performs database operations (run_query, adapter.execute)
    and therefore MUST be guarded by {% if execute %} to prevent compilation errors
    when dbt is in the parsing phase.
    #}

    {% set exclude_columns = ["load_dw_audt_ts", "updt_dw_audt_ts"] %}

    {#
    The execute variable is True when dbt is actually running SQL against the database,
    and False during the initial compilation/parsing phase.
    All database interactions (like run_query and adapter.execute)
    must be inside this if execute block.
    #}
    {% if execute %}
        {{ log("DEBUG: REUSABLE_JS_S3_REDSHIFT_copy_withdynamicschema_new_API is in EXECUTION phase for target: " ~ dataset_name ~ "." ~ table_name, info=true) }}

        -- Step 1: Fetch column names for the target table
        {% set target_cols_query %}
            SELECT column_name
            FROM {{ dataset_name }}.INFORMATION_SCHEMA.COLUMNS
            WHERE table_name = '{{ table_name }}'
            AND column_name NOT IN ({{ exclude_columns | map('tojson') | join(', ') }})
        {% endset %}

        {#
        Execute the query to get target columns.
        run_query returns an Agate table object during execution,
        but None during compilation.
        #}
        {% set target_cols_result = run_query(target_cols_query) %}

        {# Validate if the query returned results before attempting to access columns #}
        {% if target_cols_result is none or target_cols_result.columns | length == 0 %}
            {% do exceptions.raise_compiler_error("Error in REUSABLE_JS_S3_REDSHIFT_copy_withdynamicschema_new_API: Target columns query returned no result or no columns for " ~ dataset_name ~ "." ~ table_name) %}
        {% endif %}

        {# Extract column names from the query result #}
        {% set target_cols = target_cols_result.columns[0].values() | join(', ') %}
        {{ log("DEBUG: Target columns fetched: " ~ target_cols, info=true) }}

        -- Step 2: Fetch column names for the source table
        {% set source_cols_query %}
            SELECT column_name
            FROM {{ dataset_name }}.INFORMATION_SCHEMA.COLUMNS
            WHERE table_name = '{{ source_table }}'
            AND column_name NOT IN ({{ exclude_columns | map('tojson') | join(', ') }})
        {% endset %}

        {# Execute the query to get source columns #}
        {% set source_cols_result = run_query(source_cols_query) %}

        {# Validate if the query returned results #}
        {% if source_cols_result is none or source_cols_result.columns | length == 0 %}
            {% do exceptions.raise_compiler_error("Error in copy_with_dynamic_schema: Source columns query returned no result or no columns for " ~ dataset_name ~ "." ~ source_table) %}
        {% endif %}

        {# Extract column names from the query result #}
        {% set source_cols = source_cols_result.columns[0].values() | join(', ') %}
        {{ log("DEBUG: Source columns fetched: " ~ source_cols, info=true) }}

        -- Step 3: Optionally truncate the target table
        {% if is_delete == 1 %}
            {% set truncate_sql %}
                TRUNCATE TABLE {{ dataset_name }}.{{ table_name }};
            {% endset %}
            {{ log("Truncating table: " ~ dataset_name ~ "." ~ table_name, info=True) }}
            {# adapter.execute performs DML and must be inside if execute #}
            {% do adapter.execute(truncate_sql) %}
        {% endif %}

        -- Step 4: Build and execute the insert query
        {% set insert_sql %}
            INSERT INTO {{ dataset_name }}.{{ table_name }} ({{ target_cols }})
            SELECT {{ source_cols }}
            FROM {{ dataset_name }}.{{ source_table }};
        {% endset %}

        {{ log("Running insert: " ~ insert_sql, info=True) }}
        {# adapter.execute performs DML and must be inside if execute #}
        {% do adapter.execute(insert_sql) %}

    {% else %}
        {#
        This block runs during the compilation/parsing phase when execute is False.
        No database interactions should occur here.
        We log a message to indicate that the macro is being parsed but not executed.
        #}
        {{ log("DEBUG: copy_with_dynamic_schema is in COMPILATION phase. Skipping database operations.", info=true) }}
    {% endif %}

{% endmacro %}