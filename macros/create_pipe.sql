{%- macro create_pipe(
  pipe_name,
  schema_name,
  table_name,
  stage_name,
  integration_name,
  s3_url,
  columns,
  file_type,
  pattern=None
  )
-%}


{%- set sql -%}
begin;

-- Create schema
create schema if not exists {{ schema_name }};

-- Create stage
create or replace stage {{ schema_name }}.{{ stage_name }}
  storage_integration = {{ integration_name }}
  url = "{{ s3_url }}";

-- Create table
create or replace table {{ schema_name }}.{{ table_name }} (
  {%- for col, type in columns.items() %}
  {{ ', ' if not loop.first }}{{ col }} {{ type }}
  {%- endfor %}
  , file_name text
  , file_id text
  , row_number integer
  , load_time timestamp
);

-- Load historic data
copy into {{ schema_name }}.{{ table_name }}
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
    , current_timestamp
  from
    @{{ schema_name }}.{{ stage_name }}
)
on_error = continue
{{ "pattern = '"~pattern~"'" if pattern }}
file_format = (
  type = {{ file_type }}
  {%- if file_type == 'CSV' %}
    skip_header = 1
    field_optionally_enclosed_by = '"'
    null_if = ('', 'null')
  {% endif -%}
);


-- Create pipe
create or replace pipe {{ schema_name }}.{{ pipe_name }} auto_ingest = true as
  copy into {{ schema_name }}.{{ table_name }}
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
    , current_timestamp
  from
    @{{ schema_name }}.{{ stage_name }}
  )
  on_error = continue
  {{ "pattern = '"~pattern~"'" if pattern }}
  file_format = (
    type = {{ file_type }}
    {%- if file_type == 'CSV' %}
      skip_header = 1
      field_optionally_enclosed_by = '"'
      null_if = ('', 'null')
    {% endif -%}
  );

commit;
{%- endset -%}

{%- do log(sql, info=True) -%}
{% if not var('DRY_RUN', True) %}
  {%- do run_query(sql) -%}
  {%- do log('Auto-ingest pipe created: '~pipe_name, info=True) -%}
{% endif %}
{%- endmacro -%}
