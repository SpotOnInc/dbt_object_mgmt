{%- macro create_pipe(file) -%}

{% if not file %}
  {{ exceptions.raise_compiler_error(
    "\nyou must pass in a file via arguments:" ~
    "\n  --args 'file: ./snowflake/snowpipe/s3_pipe_jaffle_shop_customers.yml'"
  ) }}
{% endif %}

{% set pipe = gather_results(file) %}
{%- set database_name = pipe.database_name or target.database %}
{%- set schema_name = database_name ~ '.' ~ pipe.schema_name %}
{%- set table_name = pipe.table_name %}
{%- set file_type = pipe.file_type %}

{# set some defaults #}
{%- set format_type_options = {
    'skip_header': 1,
    'null_if': ('', 'null'),
  }
  if file_type == 'CSV'
  else {}
%}

{% if pipe.extra_format_options %}
  {{ format_type_options.update(pipe.extra_format_options) }}
{% endif %}

{% set copy_statement -%}
copy into {{ schema_name }}.{{ table_name }} from (
    select
      {%- if file_type == 'JSON' %}
      parse_json($1)
      {%- else %}
      {%- for col in pipe.columns %}
      {{ ', ' if not loop.first }}${{ loop.index }}
      {%- endfor %}
      {%- endif %}
      , metadata$filename
      , md5(metadata$filename)
      , metadata$file_row_number
      , metadata$file_last_modified
      , metadata$start_scan_time
    from
      @{{ schema_name }}.{{ table_name }}_stage
  )
  on_error = continue
  {{ "pattern = '" ~ pipe.pattern ~ "'" if pipe.pattern }}
  file_format = (
    type = '{{ file_type }}'
    {% for key, value in format_type_options.items() %}
      {{- key }} = {{ value }}
    {% endfor -%}
    )
{% endset %}


{%- set sql -%}
begin name create_pipe;

-- Create schema
create schema if not exists {{ schema_name }};

-- Create stage
create or replace stage {{ schema_name }}.{{ table_name }}_stage
  storage_integration = {{ pipe.integration_name }}
  url = '{{ pipe.s3_url }}'
;

-- Create table
create or replace table {{ schema_name }}.{{ table_name }} (
  {%- for col, type in pipe.columns.items() %}
  {{ ', ' if not loop.first }}{{ col }} {{ type }}
  {%- endfor %}
  , file_name text
  , file_id text
  , row_number integer
  , last_modified_time timestamp_ntz
  , load_time timestamp_ltz
  )
;

-- First load historic data
{{ copy_statement }}
;

-- Create pipe
create or replace pipe {{ schema_name }}.{{ table_name }}_pipe auto_ingest = true as
{{ copy_statement }}
;

commit;
{%- endset -%}

{%- do log(sql, info=True) -%}
{% if not var('dry_run', False) %}
  {%- do run_query(sql) -%}
{% endif %}
{%- endmacro -%}
