{% macro aggregate_row_errors(error_expressions) %}
    array_to_string(
        (
            select array_agg(msg ignore nulls)
            from unnest(array[
                {{ error_expressions | join(', ') }}
            ]) as msg
        ),
        ' | '
    )
{% endmacro %}
