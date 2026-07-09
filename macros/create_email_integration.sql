{%- macro create_email_integration(file) -%}

{% if not file %}
  {{ exceptions.raise_compiler_error(
    "\nyou must pass in a file via arguments:" ~
    "\n  --args 'file: ./snowflake/integration/example_email_integration.yml'"
  ) }}
{% endif %}

{% set config = dbt_object_mgmt.gather_results(file) %}

{# integration_name is the only required field #}
{% set integration_name = config.pop('integration_name', None) %}
{% if not integration_name %}
  {{ exceptions.raise_compiler_error(
    "\n'integration_name' is required in " ~ file
  ) }}
{% endif %}

{# enabled defaults to true and must be a real boolean so bad values fail loudly #}
{% set enabled = config.pop('enabled', True) %}
{% if enabled is not boolean %}
  {{ exceptions.raise_compiler_error(
    "\n'enabled' must be a boolean (true/false) in " ~ file ~ ", got: " ~ enabled
  ) }}
{% endif %}

{% set allowed_recipients = config.pop('allowed_recipients', None) %}
{% set default_recipients = config.pop('default_recipients', None) %}
{% set default_subject = config.pop('default_subject', None) %}
{% set comment = config.pop('comment', None) %}

{# recipient parameters must be lists of addresses, not bare strings #}
{% for key, value in {
    'allowed_recipients': allowed_recipients,
    'default_recipients': default_recipients
  }.items() if value is not none %}
  {% if value is string or value is not iterable %}
    {{ exceptions.raise_compiler_error(
      "\n'" ~ key ~ "' must be a list of email addresses in " ~ file
    ) }}
  {% endif %}
{% endfor %}

{# any leftover keys are typos or unsupported params - surface them at compile time #}
{% if config %}
  {{ exceptions.raise_compiler_error(
    "\nunsupported email integration parameter(s) in " ~ file ~ ": " ~
    (config.keys() | list | join(', ')) ~
    "\nsupported: integration_name, enabled, allowed_recipients," ~
    " default_recipients, default_subject, comment"
  ) }}
{% endif %}

{# attributes shared by CREATE and ALTER - type is create-only and cannot be altered #}
{% set set_attributes %}
  enabled = {{ enabled }}
  {%- if allowed_recipients is not none %}
  allowed_recipients = ({{ "'" + allowed_recipients | join("', '") + "'" }})
  {%- endif %}
  {%- if default_recipients is not none %}
  default_recipients = ({{ "'" + default_recipients | join("', '") + "'" }})
  {%- endif %}
  {%- if default_subject is not none %}
  default_subject = '{{ default_subject | replace("'", "''") }}'
  {%- endif %}
  {%- if comment is not none %}
  comment = '{{ comment | replace("'", "''") }}'
  {%- endif %}
{% endset %}

{%- set sql -%}
begin name create_email_integration;

{# create if it does not exist so we never drop an existing integration #}
create notification integration if not exists {{ integration_name }}
  type = email
  {{- set_attributes }}
;

{# alter keeps an existing integration in sync with the config on every run #}
alter notification integration {{ integration_name }} set
  {{- set_attributes }}
;

commit;
{%- endset -%}

{{ log(sql, info=True) }}
{% if not var('dry_run', False) %}
  {{ run_query(sql) }}
{% endif %}

{%- endmacro -%}
