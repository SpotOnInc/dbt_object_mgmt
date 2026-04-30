{% macro create_roles() %}

{% set file = var('snowflake_role_file', 'snowflake/roles/roles.yml') %}

{% set role_list = dbt_object_mgmt.gather_results(file) %}

{%- set role_sql -%}

-- set single transaction to rollback if errors
begin name create_roles;
use role {{ var('snowflake_admin', 'SECURITYADMIN') }};

{% for role in role_list -%}

  {%- set role_name    = role.get('name') | upper %}
  {%- set comment      = role.get('comment', '') %}
  {%- set parent_roles = role.get('parent_roles', []) %}
  {%- set grants       = role.get('grants', []) %}

  create role if not exists {{ role_name }};

  {%- if comment %}
  alter role {{ role_name }} set comment = '{{ comment | trim }}';
  {%- endif %}
  
  {% for parent_role in parent_roles -%}
  grant role {{ role_name }} to role {{ parent_role | upper }};
  {% endfor -%}

  {% for grant in grants -%}
  {%- set privilege = grant.get('privilege') | upper %}
  {%- set objects   = grant.get('objects', []) %}

  {% for obj in objects -%}
  {%- set object_type = obj.get('type') | upper %}
  {%- set names       = obj.get('names', []) %}
  {%- set names       = ([names] if names is string else names) %}
  {%- set in_scopes   = obj.get('in', []) %}
  {%- set in_scopes   = ([in_scopes] if in_scopes is string else in_scopes) %}

  {%- if object_type == 'ACCOUNT' %}
    grant {{ privilege }} on account to role {{ role_name }};
  {%- elif in_scopes %}
    {% for scope in in_scopes -%}
      grant {{ privilege }} on {{ object_type }} in {{ scope | upper }} to role {{ role_name }};
    {% endfor -%}
  {%- else %}
    {% for name in names -%}
      grant {{ privilege }} on {{ object_type }} {{ name | upper }} to role {{ role_name }};
    {% endfor -%}
  {%- endif %}
  {% endfor -%}
  {% endfor %}
{%- endfor %}

commit;

{%- endset %}

{% do log(role_sql, info=True) %}
{% if not var('dry_run', False) %}
  {{ run_query(role_sql) }}
{% endif %}

{% endmacro %}
