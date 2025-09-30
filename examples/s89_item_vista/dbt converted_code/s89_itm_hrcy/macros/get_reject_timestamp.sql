-- macros/utils/get_reject_timestamp.sql
{% macro get_reject_timestamp() %}
    -- For BigQuery (assuming BigQuery based on previous context)
    FORMAT_DATETIME('%Y%m%d_%H%M%S', CURRENT_DATETIME())
    -- For other databases, adjust the formatting function:
    -- PostgreSQL: to_char(current_timestamp, 'YYYYMMDD_HH24MISS')
    -- Snowflake: to_char(current_timestamp, 'YYYYMMDD_HH24MISS')
    -- Redshift: to_char(GETDATE(), 'YYYYMMDD_HH24MISS')
{% endmacro %}