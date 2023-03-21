{%- macro create_integration(file) -%}

{% if not file %}
  {{ exceptions.raise_compiler_error(
    "\nyou must pass in a file via arguments:" ~
    "\n  --args 'file: ./snowflake/integration/s3_to_snowflake_integration.yml'"
  ) }}
{% endif %}

{% set results = gather_results(file) %}

{# must parse here - dbt returns strings through functions :( #}
{% if get_file_type(file) == 'json' %}
  {% set integration = fromjson(results) %}
{% else %}
  {% set integration = fromyaml(results) %}
{% endif %}

{% set integration_name = integration.pop('integration_name') %}
{% set integration_type = integration.pop('integration_type') %}
{% set attributes %}
  {%- for key, value in integration.items() %}
    {%- if value is iterable and value is not string %}
      {{ key }} = (
        {%- for loc in value %}
        {{ loc }}{% if not loop.last %},{% endif %}
        {%- endfor %}
      )
    {%- else %}
      {{ key }} = {{ value }}
    {%- endif %}
  {%- endfor %}
{% endset %}


{%- set sql -%}
begin name create_integration;

{# don't want to delete if already exists :) #}
create {{ integration_type }} integration if not exists {{ integration_name }}
{{- attributes }}
;

alter storage integration {{ integration_name }} set
{{- attributes }}
;

commit;
{%- endset -%}

{{ log(sql, info=True) }}
{% if not var('DRY_RUN', False) %}
  {{ run_query(sql) }}
{% endif %}

{%- endmacro -%}
