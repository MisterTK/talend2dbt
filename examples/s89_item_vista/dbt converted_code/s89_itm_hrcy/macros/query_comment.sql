{% macro query_comment(node, dag_id, dag_dw_job_id) %}
    {%- set comment_dict = {} -%}
    {%- do comment_dict.update(
        app='dbt',
        dbt_version=dbt_version,
        profile_name=target.get('profile_name'),
        target_name=target.get('target_name')
    ) -%}
    {%- if node is not none -%}
      {%- do comment_dict.update(
        file=node.original_file_path,
        dag_id=dag_id
      ) -%}
    {%- else -%}
      {%- do comment_dict.update(node_id='internal') -%}
    {%- endif -%}
    {%- do comment_dict.update(dag_dw_job_id=dag_dw_job_id) -%}
    {% do return(tojson(comment_dict)|string()) %}
{% endmacro %}