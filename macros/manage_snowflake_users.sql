{%- macro manage_snowflake_users (
    users,
    dry_run=True
  )
-%}

{# get a list of current non-deleted snowflake users #}
{%- call statement('user_query', fetch_result=True) -%}
    select lower(name) -- lower helps string matching
    from snowflake.account_usage.users
    where deleted_on is null -- currently active users
    and name != 'SNOWFLAKE' -- don't want to delete
{%- endcall -%}

{%- set current_users = load_result('user_query')['data'] |
    map(attribute=0) | list -%}

{%- set yml_user_names = users | map(attribute='name') |
    map('lower') | list %}

{%- set _password = var('PASSWORD') %}

{%- set user_sql -%}
begin name create_users; -- set single transaction to rollback if errors

use role accountadmin;

{% for user in users -%}

{%- set name = user.get('name') %}
{%- set attributes = user.get('attributes') %}
{%- set roles = user.get('roles', 'ANALYTICS') %}

{#- lower helps with string matching #}
{%- if name.lower() not in current_users %}
create user
  if not exists {{ name }}
  password = '{{ _password }}'
  must_change_password = true
;
{%- endif %}

alter user {{ name }} set
  {% for key, value in attributes.items() -%}
  {%- if key in ["display_name", "first_name", "last_name"] -%}
  {{ key }} = {{ "'" ~ value.title() ~ "'" }}
  {%- elif key not in ["comment", "password", "login_name", "email"] -%}
  {{ key }} = {{ value }}
  {%- else -%}
  {{ key }} = {{ "'" ~ value ~ "'" }}
  {%- endif %}
  {% endfor -%}
;

{#- this enables google as a login #}
{% if '@' in attributes['login_name'].lower() %}
alter user {{name}} unset password;
{%- endif %}

{% for role in roles -%}
grant role
  {{ role }} to
  user {{ name }}
;
{% endfor -%}
{%- endfor -%}

commit;

{%- endset %}


{%- do log(user_sql, info=True) -%}

{% if not var('DRY_RUN', True) %}
  {%- do log('Executing user statements...', info=True) -%}
  {#- do run_query(user_sql) -#}
{% endif %}

{%- endmacro -%}
