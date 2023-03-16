{% macro create_users() %}

{% set file = var('snowflake_user_file' , 'snowflake/users/users.yml') %}
{% set file_type = file.split('.')[-1] %}
{%- set _password = var('PASSWORD', 's0up3rs$cr3t') %}
{% set PARSERS = {
  'yml': user_yml_parser(),
  'csv': user_csv_parser(),
  'json': user_json_parser(),
  }
%}

{% if file_type not in PARSERS.keys() %}
  {% do exceptions.raise_compiler_error(
  '\nfile format needs to be in ' ~ ', '.join(PARSERS.keys()) ~
  ' not ' ~ file_type
) %}
{% endif %}

{{ put_file(file) }}


{# queries to create file format and queries #}

{% set format_args, user_sql = PARSERS[file_type] %}

{{ create_file_format(format_args) }}
{% set results = run_query(user_sql) %}


{# create a dictionary with user values #}
{% set user_dict = {} %}

{% for user, _type, _key, _value in results %}

  {% if user not in user_dict %}
    {{ user_dict.update({ user: {'attributes': {}, 'roles': [] } })}}
  {% endif %}

  {% if _value %}
    {{ user_dict[user][_type].update({_key: _value}) }}
  {% else %}
    {{ user_dict[user][_type].append(_key) }}
  {% endif %}

{% endfor %}



{%- set user_sql -%}

-- set single transaction to rollback if errors
begin name create_users;
use role {{ var('snowflake_admin', 'securityadmin') }};

{% for user, values in user_dict.items() -%}

{%- set attributes = values.get('attributes') -%}
{%- set roles = values.get('roles', ['ANALYTICS']) %}

create user
  if not exists {{ user }}
  password = '{{ _password }}'
  must_change_password = true
;

alter user {{ user }} set
  {% for key, value in attributes.items() -%}
  {# we don't want to force capitalization #}
  {%- if key not in ["comment", "password", "login_name", "email"] -%}
  {{ key }} = {{ value }}
  {%- else -%}
  {{ key }} = {{ "'" ~ value ~ "'" }}
  {%- endif %}
  {% endfor -%}
;

{% for role in roles -%}
grant role
  {{ role }} to
  user {{ user }}
;
{% endfor -%}
{%- endfor -%}

commit;

{%- endset %}


{% do log(user_sql, info=True) %}

{% do log('Executing user statements...', info=True) %}
{# {{ run_query(user_sql) }} #}

{% endmacro %}
