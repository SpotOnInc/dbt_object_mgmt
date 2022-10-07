{%- macro create_whitelist(
  network_policy,
  comment,
  policy_lists)
-%}

{%- set network_policy_sql -%}
begin;

create network policy if not exists
  {{ network_policy }}
  allowed_ip_list = ('0.0.0.0/0')
;

alter network policy
  {{ network_policy }} set
  {% for param, policy_list in policy_lists.items() -%}

  {%- set ip_list = [] -%}

    {%- for ip in policy_list -%}
      {%- do ip_list.append("'" ~ ip ~ "'") -%}
    {%- endfor -%}

  {{ param }} = ({{ ','.join(ip_list) }})
  {%- if not loop.last %}{{ '\n  ' }}{% endif %}
  {%- endfor %}
  comment = '{{ comment }}'
;

commit;

{%- endset %}


{%- do log(network_policy_sql, info=True) -%}

{% if not var('DRY_RUN', True) %}
  {%- do log("\nrunning network policy " ~ network_policy ~ "\n", info=True) -%}
  {%- do run_query(network_policy_sql) -%}
{% endif %}


{%- endmacro -%}
