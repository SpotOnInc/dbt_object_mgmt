{% macro create_network_policies() %}

{% set file = var('snowflake_network_policy_file' , 'snowflake/whitelist/network_policies.yml') %}

{% set policy_list = dbt_object_mgmt.gather_results(file) %}


{% set network_policy_sql %}

-- set single transaction to rollback if errors
begin name create_policy;
use role {{ var('snowflake_admin', 'securityadmin') }};

{% for policy in policy_list %}
  {% set policy_name = policy.pop('network_policy') %}

  create network policy if not exists
    {{ policy_name }}
    allowed_ip_list = ('0.0.0.0/0')
  ;

  alter network policy
    {{ policy_name }} set
    {% for key, value in policy.items() -%}
    {%- if value is not string %}
      {{- key }} = ({{ "\'" + value | join("\', \'") + "\'" }})
    {%- else %}
      {{- key }} = '{{ value }}'
    {%- endif %}
    {% endfor %}
  ;

{%- endfor %}

commit;
{% endset %}

{{ log(network_policy_sql, info=True) }}
{% if not var('dry_run', False) %}
  {{ run_query(network_policy_sql) }}
{% endif %}

{% endmacro %}
