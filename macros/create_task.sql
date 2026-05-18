{% macro create_task(file) %}

{{ dbt_object_mgmt._require_file(file, './snowflake/snowpipe/s3_pipe_jaffle_shop_customers.yml') }}

{% set task = dbt_object_mgmt.gather_results(file) %}

{# set the non-standard kay:values #}
{% set task_name = task.pop('task_name') %}
{% set sql_block = task.pop('sql', 'select 1') %}
{% set after = task.pop('after', None) %}
{% set when = task.pop('when', None) %}
{% set enabled = task.pop('enabled', True) %}


{% set task_sql %}
create or alter task {{ task_name }}
  {%- for key, value in task.items() %}
  {{ key }} = {{ value }}
  {%- endfor %}
  as
    {{ sql_block }}
  {{ 'after '~ after if after else '' -}}
  {{ 'when '~ when if when else '' -}}
;

alter task {{ task_name }} {{ 'resume' if enabled else 'suspend' }};
{% endset %}

{{ print(task_sql) }}

{% endmacro %}
