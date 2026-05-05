{%- macro create_mcp_server(file) -%}

{% if not file %}
  {{ exceptions.raise_compiler_error(
    "\nyou must pass in a file via arguments:" ~
    "\n  --args 'file: ./snowflake/mcp_server/my_mcp_server.yml'"
  ) }}
{% endif %}

{% set config = dbt_object_mgmt.gather_results(file) %}

{# qualify server_name to db.schema.name, db.name, or as-is depending on dot count #}
{%- set server_name = config.pop('server_name') %}
{%- set prefix = {
    1: target.database ~ '.' ~ target.schema ~ '.',
    2: target.database ~ '.'
  }.get(server_name.split('.') | length, '') -%}
{%- set server_name = prefix ~ server_name %}


{%- set tools = config.pop('tools', []) %}

{# Build the YAML specification by constructing each line explicitly #}
{%- set spec_lines = [] %}
{%- do spec_lines.append('tools:') %}
{%- for tool in tools %}
  {%- do spec_lines.append('- name: "' ~ tool.name ~ '"') %}
  {%- do spec_lines.append('  type: "' ~ tool.type ~ '"') %}
  {%- if tool.title %}
    {%- do spec_lines.append('  title: "' ~ tool.title ~ '"') %}
  {%- endif %}
  {%- if tool.description %}
    {%- do spec_lines.append('  description: "' ~ tool.description ~ '"') %}
  {%- endif %}
  {%- if tool.identifier %}
    {%- do spec_lines.append('  identifier: "' ~ tool.identifier ~ '"') %}
  {%- endif %}
  {%- if tool.config %}
    {%- set tool_config = tool.config %}
    {%- do spec_lines.append('  config:') %}
    {%- do spec_lines.append('    type: "' ~ tool_config.type ~ '"') %}
    {%- if tool_config.warehouse %}
      {%- do spec_lines.append('    warehouse: "' ~ tool_config.warehouse ~ '"') %}
    {%- endif %}
    {%- if tool_config.input_schema %}
      {%- set input_schema = tool_config.input_schema %}
      {%- do spec_lines.append('    input_schema:') %}
      {%- do spec_lines.append('      type: "' ~ input_schema.type ~ '"') %}
      {%- if input_schema.properties %}
        {%- do spec_lines.append('      properties:') %}
        {%- for prop_name, prop in input_schema.properties.items() %}
          {%- do spec_lines.append('        ' ~ prop_name ~ ':') %}
          {%- if prop.description %}
            {%- do spec_lines.append('          description: "' ~ prop.description ~ '"') %}
          {%- endif %}
          {%- if prop.type %}
            {%- do spec_lines.append('          type: "' ~ prop.type ~ '"') %}
          {%- endif %}
        {%- endfor %}
      {%- endif %}
    {%- endif %}
  {%- endif %}
{%- endfor %}

{%- set spec_yaml = spec_lines | join('\n') %}

{%- set sql %}
create or replace mcp server {{ server_name }}
from specification $$
{{ spec_yaml }}
$$;
{%- endset %}

{{ log(sql, info=True) }}
{% if not var('dry_run', False) %}
  {{ run_query(sql) }}
{% endif %}

{%- endmacro -%}
