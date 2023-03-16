{% macro network_yml_parser() %}

{% set format_args %}
  type = 'CSV'
  record_delimiter = '\n'
{% endset %}

{# maybe there's a better way of parsing out yml with sql #}
{% set network_sql %}

  with raw_data as (

    select
      metadata$file_row_number as _row
      , len(regexp_substr($1, '^\\s{1,}')) as _indentation
      , min(_indentation) over () as _indentation_size
      , conditional_true_event(_indentation is null) over (order by _row) as _number
      , split(regexp_replace($1, '^[\\s|-]{1,}', ''), ': ') as split_attributes
      , split_attributes[0] as first_attribute
      , iff(contains(first_attribute, 'network_policy'), split_attributes[1], null)::text as _policy_name
      , iff(_indentation = _indentation_size, first_attribute, null)::text as first_key
      , split_attributes[array_size(split_attributes) - 1]::text as _value
    from @{{ get_stage_name() }} (
      file_format => {{ get_file_format() }}
    )

  )

  select
    lag(_policy_name) ignore nulls over (partition by _number order by _row) as policy 
    , lag(replace(first_key, ':', ''), 1, first_attribute) ignore nulls over (partition by _number order by _row) as _type
    , _value
  from
    raw_data
  where not contains(_value, '#') -- yaml comments
  qualify policy is not null and not contains(_value, '_ip_list')

{% endset %}

{{ return((format_args, network_sql)) }}
{% endmacro %}