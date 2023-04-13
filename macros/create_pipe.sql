{%- macro create_pipe(file) -%}

{% if not file %}
  {{ exceptions.raise_compiler_error(
    "\nyou must pass in a file via arguments:" ~
    "\n  --args 'file: ./snowflake/snowpipe/s3_pipe_jaffle_shop_customers.yml'"
  ) }}
{% endif %}

{% set results = gather_results(file) %}

{# must parse here - dbt returns strings through functions :( #}
{% if get_file_type(file) == 'json' %}
  {% set pipe = fromjson(results) %}
{% else %}
  {% set pipe = fromyaml(results) %}
{% endif %}

{%- set schema_name = target.database ~ '.' ~ pipe.schema_name %}
{%- set table_name = pipe.table_name %}
{%- set file_type = pipe.file_type %}
{% set copy_attributes %}
  from (
    select
      {%- if file_type == 'JSON' %}
      parse_json($1)
      {%- else %}
      {%- for col in columns %}
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
  {{- "pattern = '" ~ pipe.pattern ~ "'" if pipe.pattern }}
  file_format = (
    type = {{ file_type }}
    {%- if file_type == 'CSV' %}
    skip_header = 1
    field_optionally_enclosed_by = '"'
    null_if = ('', 'null')
    {% endif -%}
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

-- Load historic data
copy into {{ schema_name }}.{{ table_name }}
{{- copy_attributes }}
;

-- Create pipe
create or replace pipe {{ schema_name }}.{{ table_name }}_pipe auto_ingest = true as
copy into {{ database }}.{{ schema_name }}.{{ table_name }}
{{- copy_attributes }}
;

commit;
{%- endset -%}

{%- do log(sql, info=True) -%}
{% if not var('dry_run', False) %}
  {%- do run_query(sql) -%}
{% endif %}
{%- endmacro -%}
