{% macro create_users() %}

{% set file = var('snowflake_user_file' , 'snowflake/whitelist/network_policies.yml') %}
{%- set _password = var('password', 's0up3rs$cr3t') %}
{% set must_quote_columns = ['email', 'comment', 'rsa_public_key'] %}

{% set result_list = gather_results(file) %}

{# must parse here - dbt returns strings through functions :( #}
{% if get_file_type(file) == 'json' %}
  {% set user_list = fromjson(result_list) %}
{% else %}
  {% set user_list = fromyaml(result_list) %}
{% endif %}



{%- set user_sql -%}

-- set single transaction to rollback if errors
begin name create_users;
use role {{ var('snowflake_admin', 'securityadmin') }};

{% for user in user_list -%}

  {%- set user_name = user.pop('name') %}
  {%- set attributes = user.get('attributes') -%}
  {%- set roles = user.get('roles', ['PUBLIC']) %}

  create user
    if not exists {{ user_name }}
    password = '{{ _password }}'
    must_change_password = true
  ;

  alter user {{ user_name }} set
    {% for key, value in attributes.items() -%}
    {%- if key in must_quote_columns -%}
    {{ key }} = {{ "'" ~ value ~ "'" }}
    {%- elif key.split('_')[0] == 'default' -%}
    {{ key }} = {{ value.upper() }}
    {%- else -%}
    {{ key }} = {{ value }}
    {%- endif %}
    {% endfor -%}
  ;

  {% for role in roles -%}
  grant role
    {{ role.upper() }} to
    user {{ user_name }}
  ;
  {% endfor -%}

{%- endfor -%}

commit;

{%- endset %}


{% do log(user_sql, info=True) %}
{% if not var('dry_run', False) %}
  {{ run_query(user_sql) }}
{% endif %}

{% endmacro %}
