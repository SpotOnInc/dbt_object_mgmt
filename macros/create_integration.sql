{%- macro create_integration(file) -%}

{% if not file %}
  {{ exceptions.raise_compiler_error(
    "\nyou must pass in a file via arguments:" ~
    "\n  --args 'file: ./snowflake/integration/s3_to_snowflake_integration.yml'"
  ) }}
{% endif %}

{% set integration = gather_results(file) %}

{% set integration_name = integration.pop('integration_name') %}
{% set integration_type = integration.pop('integration_type') %}
{% set create_attributes %}
  {%- for key, value in integration.items() %}
    {%- if value is iterable and value is not string %}
      {{ key }} = ({{ "\'" + value | join("\', \'") + "\'" }})
    {%- else %}
      {{ key }} = {{ value }}
    {%- endif %}
  {%- endfor %}
{% endset %}

{% set alter_attributes %}
  {%- for key, value in integration.items() if key not in ['type', 'storage_provider'] %}
    {%- if value is iterable and value is not string %}
      {{ key }} = ({{ "\'" + value | join("\', \'") + "\'" }})
    {%- else %}
      {{ key }} = {{ value }}
    {%- endif %}
  {%- endfor %}
{% endset %}


{%- set sql -%}
begin name create_integration;

{# don't want to delete if already exists :) #}
create {{ integration_type }} integration if not exists {{ integration_name }}
{{- create_attributes }}
;

alter {{ integration_type }} integration {{ integration_name }} set
{{- alter_attributes }}
;

commit;
{%- endset -%}

{{ run_it(sql) }}

{%- endmacro -%}
