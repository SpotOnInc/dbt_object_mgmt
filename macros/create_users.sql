{% macro create_users() %}

{% set file = var('snowflake_user_file' , 'snowflake/users/users.yml') %}
{%- set _password = var('password', 's0up3rs$cr3t') %}
{% set must_quote_columns = ['email', 'comment', 'rsa_public_key', 'display_name', 'first_name', 'last_name', 'middle_name'] %}

{% set user_list = dbt_object_mgmt.gather_results(file) %}


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
    {%- if key in must_quote_columns or ' ' in value|string -%}
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
