{% macro create_network_policy() %}

{% set file = var('snowflake_network_policy_file' , 'snowflake/whitelist/network_policies.yml') %}
{% set file_type = file.split('.')[-1] %}
{% set PARSERS = {
  'yml': network_yml_parser()
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

{% set format_args, network_sql = PARSERS[file_type] %}

{{ create_file_format(format_args) }}
{% set results = run_query(network_sql) %}


{# create a dictionary for policies #}
{% set network_dict = {} %}

{% for policy, _type, _value in results %}

  {% if policy not in network_dict %}
    {{ network_dict.update({ policy: {
        'allowed_ip_list': [],
        'blocked_ip_list': []
        }
      })
    }}
  {% endif %}

  {% if _type == 'comment' %}
    {{ network_dict[policy].update({_type: _value}) }}
  {% else %}
    {{ network_dict[policy][_type].append(_value) }}
  {% endif %}

{% endfor %}


{% set sql %}
  
-- set single transaction to rollback if errors
begin name create_policy;
use role {{ var('snowflake_admin', 'securityadmin') }};

{% for network_policy, values in network_dict.items() %}

  create network policy if not exists
    {{ network_policy }}
    allowed_ip_list = ('0.0.0.0/0')
  ;

  alter network policy
    {{ network_policy }} set
    {% for _key, _value in values.items() -%}
    {%- if not _value -%}
      -- nothing exists for {{ _key }}
    {%- elif _value is not string %}
      {{- _key }} = ({{ "\'" + _value | join("\', \'") + "\'" }})
    {%- else %}
      {{- _key }} = '{{ _value }}'
    {%- endif %}
    {% endfor %}
  ;

commit;

{%- endfor %}
{% endset %}

{{ log(sql, info=True) }}
{% if not var('DRY_RUN', True) %}
  {{- run_query(network_policy_sql) -}}
{% endif %}

{% endmacro %}
