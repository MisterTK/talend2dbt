{% macro get_dynamic_date_range() %}
    {#
    This macro replicates the Talend tJava logic for date range calculation.
    It prioritizes explicit from_dt_input/to_dt_input.
    If not provided, it uses no_of_days_input to calculate a range ending today.
    Expected var formats:
    - from_dt_input: 'DDMMYYYY' string or null
    - to_dt_input: 'DDMMYYYY' string or null
    - no_of_days_input: integer or null
    #}

    {% set from_dt = var('from_dt_input', None) %}
    {% set to_dt = var('to_dt_input', None) %}
    {% set no_of_days = var('no_of_days_input', None) %}

    {% set final_from_dt = '' %}
    {% set final_to_dt = '' %}

    {% if from_dt and to_dt and no_of_days is none %}
        {# Scenario 1: Explicit from_dt and to_dt provided #}
        {% set final_from_dt = modules.datetime.datetime.strptime(from_dt, '%d%m%Y').strftime('%Y-%m-%d') %}
        {% set final_to_dt = modules.datetime.datetime.strptime(to_dt, '%d%m%Y').strftime('%Y-%m-%d') %}

    {% elif (from_dt is none or from_dt == '') and (to_dt is none or to_dt == '') and no_of_days is not none %}
        {# Scenario 2: No dates, but no_of_days provided #}
        {# current_timestamp is a dbt helper that gets the current time in the target DB's timezone #}
        {% set current_date = (run_started_at | dbt_date.format('YYYY-MM-DD')) %}
        {% set calculated_from_dt = (run_started_at - modules.timedelta(days=no_of_days)) | dbt_date.format('YYYY-MM-DD') %}

        {% set final_from_dt = calculated_from_dt %}
        {% set final_to_dt = current_date %}

    {% else %}
        {# Invalid input combination #}
        {% do exceptions.raise_compiler_error("PROVIDE VALID INPUTS for date range (from_dt_input & to_dt_input OR no_of_days_input)") %}
    {% endif %}

    {{ return({"from_dt": final_from_dt, "to_dt": final_to_dt}) }}

{% endmacro %}