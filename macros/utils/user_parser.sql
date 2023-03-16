{% macro user_yml_parser() %}

{% set format_args %}
  type = 'CSV'
  record_delimiter = '\n'
{% endset %}

{# maybe there's a better way of parsing out yml with sql #}
{% set user_sql %}

  with raw_data as (

    select
      metadata$file_row_number as _row
      , conditional_true_event(contains($1, '- name:')) over (order by _row) as user_number
      , len(regexp_substr($1, '^\\s{1,}')) as indentation
      , split(replace($1, ':', ''), ' ') as text_array
      , split(replace($1, ' - ', ''), ':') as split_attributes
      , min(indentation) over (order by _row) as indentation_size
      , iff(indentation = indentation_size, text_array[array_size(text_array) - 1], null)::text as _user_name
      , iff(indentation = indentation_size * 2, text_array[array_size(text_array) - 1], null)::text as first_key
    from @{{ get_stage_name() }} (
      file_format => {{ get_file_format() }}
    )

  )

  select
    lag(_user_name) ignore nulls over (partition by user_number order by _row) as user_name
    , lag(first_key) ignore nulls over (partition by user_number order by _row) as _type
    , nullif(trim(split_attributes[0]::text), '') as _key
    , nullif(trim(split_attributes[1]::text), '') as _value
  from
    raw_data
  where not contains(_key, '#') -- yaml comments
  qualify _type is not null and _key != 'roles'
  ;

{% endset %}

{{ return((format_args, user_sql)) }}
{% endmacro %}


{% macro user_json_parser(args) %}

{% set format_args %}
  type = 'JSON'
{% endset %}

{% set user_sql %}

  with raw_data as (

    select
      f.value:name::text as user_name
      , f.value:attributes::variant as attributes
      , f.value:roles::array as roles
    from @{{ get_stage_name() }} (
      file_format => {{ get_file_format() }}
    ), lateral flatten (parse_json($1)) as f 

  )
  , attributes_cte as (

    select
      rd.user_name
      , 'attributes' as _type
      , f.key::text as _key
      , f.value::text as _value
    from raw_data as rd
    , lateral flatten(rd.attributes) as f

  )
  , roles_cte as (

    select
      rd.user_name
      , 'roles' as _type
      , f.value::text as _key
      , null as _value
    from raw_data as rd
    , lateral flatten(rd.roles) as f

  )

  select * from attributes_cte
  union all
  select * from roles_cte
  ;

{% endset %}

{{ return((format_args, user_sql)) }}
{% endmacro %}


{% macro user_csv_parser(args) %}

{% set format_args %}
  type = 'CSV'
  record_delimiter = '\n'
  skip_header = 1
{% endset %}

{% set user_sql %}

  with raw_data as (

    select
      $1 as user
      , $2 as _type
      , $3 as _key
      , $4 as _value
    from @{{ get_stage_name() }} (
      file_format => {{ get_file_format() }}
    )

  )

  select
    *
  from
    raw_data
  ;

{% endset %}

{{ return((format_args, user_sql)) }}
{% endmacro %}