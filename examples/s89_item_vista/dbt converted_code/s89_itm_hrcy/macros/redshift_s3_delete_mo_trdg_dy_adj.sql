-- macros/redshift_s3_delete_mo_trdg_dy_adj.sql

{% macro redshift_s3_delete_mo_trdg_dy_adj(
    target_schema='rmdw_tables', 
    target_table='mo_trdg_dy_adj', 
    s3_delete_file_name='DEL_GLMA_MO_TRDG_DY_ADJ.psv' 
) %}

    {# Get dynamic S3 path and IAM role from dbt variables #}
    {% set s3_bucket = var('global_datalake_bucket_name') %}
    {% set s3_path = var('global_datalake_path') %}
    {% set s3_subject_area = var('global_datalake_subject_area') %}
    {% set iam_role = var('redshift_gdap_iam_role') %}

    {% set s3_full_path = 's3://' ~ s3_bucket ~ '/' ~ s3_path ~ s3_subject_area ~ s3_delete_file_name %}

    {# Construct the multi-statement SQL #}
    {% set delete_sql %}
        -- 1. Create a temporary table
        CREATE TEMP TABLE tmp_mo_trdg_dy_adj
        (
            ctry_iso_nu SMALLINT,
            yr_nu       SMALLINT,
            mo_nu       SMALLINT
        );

        -- 2. Copy data from S3 into the temporary table
        COPY tmp_mo_trdg_dy_adj
        FROM '{{ s3_full_path }}'
        IAM_ROLE '{{ iam_role }}'
        FILLRECORD
        EMPTYASNULL
        DATEFORMAT AS 'YYYY-MM-DD'
        -- Assuming pipe-separated value file as '.psv' often implies this
        DELIMITER '|'; -- Add DELIMITER based on .psv
        -- For CSV: CSV IGNOREHEADER 1;
        -- If it's truly a CSV with pipe as delimiter: DELIMITER '|' CSV;


        -- 3. Delete matching records from the target table
        DELETE FROM {{ target_schema }}.{{ target_table }}
        USING tmp_mo_trdg_dy_adj
        WHERE {{ target_schema }}.{{ target_table }}.ctry_iso_nu = tmp_mo_trdg_dy_adj.ctry_iso_nu
          AND {{ target_schema }}.{{ target_table }}.yr_nu = tmp_mo_trdg_dy_adj.yr_nu
          AND {{ target_schema }}.{{ target_table }}.mo_nu = tmp_mo_trdg_dy_adj.mo_nu;

        -- Optional: Drop temp table if not auto-dropped at session end (Redshift usually auto-drops)
        -- DROP TABLE tmp_mo_trdg_dy_adj;
    {% endset %}

    {# Execute the SQL query #}
    {% do log("Executing Redshift S3 Delete: " ~ s3_full_path, info=true) %}
    {% do run_query(delete_sql) %}
    {% do log("Deletion from " ~ target_schema ~ "." ~ target_table ~ " completed.", info=true) %}

{% endmacro %}