{%- macro manage_snowflake_users(users, dry_run=True) -%}

{%- set _password = var('PASSWORD') %}

{%- set user_sql -%}
begin name create_users; -- set single transaction to rollback if errors

use role accountadmin;

{% for user in users -%}

{%- set name = user.get('name') %}
{%- set attributes = user.get('attributes') %}
{%- set roles = user.get('roles', 'ANALYTICS') %}

create user
  if not exists {{ name }}
  password = '{{ _password }}'
  must_change_password = true
;

alter user {{ name }} set
  {% for key, value in attributes.items() -%}
  {%- if key not in ["comment", "password", "login_name", "email"] -%}
  {{ key }} = {{ value }}
  {%- else -%}
  {{ key }} = '{{ value }}'
  {%- endif %}
  {% endfor -%}
;

{% for role in roles -%}
grant role
  {{ role }} to
  user {{ name }}
;
{% endfor -%}
{%- endfor -%}

commit;

{%- endset %}


{% do log(user_sql, info=True) %}

{% if not var('DRY_RUN', True) %}
  {% do log('Executing user statements...', info=True) %}
  {# do run_query(user_sql) #}
{% endif %}

{%- endmacro -%}
