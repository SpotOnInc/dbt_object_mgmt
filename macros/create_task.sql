{% macro create_task(file) %}

{% if not file %}
  {{ exceptions.raise_compiler_error(
    "\nyou must pass in a file via arguments:" ~
    "\n  --args 'file: ./snowflake/snowpipe/s3_pipe_jaffle_shop_customers.yml'"
  ) }}
{% endif %}

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
