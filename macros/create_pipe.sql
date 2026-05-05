{%- macro create_pipe(file) -%}

{{ dbt_object_mgmt._require_file(file, './snowflake/snowpipe/s3_pipe_jaffle_shop_customers.yml') }}

{% set pipe = dbt_object_mgmt.gather_results(file) %}
{%- set database_name = pipe.database_name or target.database %}
{%- set schema_name = database_name ~ '.' ~ pipe.schema_name %}
{%- set table_name = pipe.table_name %}
{%- set file_type = pipe.file_type %}

{%- set format_options = dbt_object_mgmt._build_format_options(file_type, pipe.extra_format_options) -%}

{%- set metadata_columns = dbt_object_mgmt._resolve_metadata_columns(pipe.custom_metadata_columns) -%}

{% set copy_statement -%}
  {{ dbt_object_mgmt._build_copy_statement(pipe, schema_name, metadata_columns, format_options) }}
{%- endset %}

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
  {%- for col_name, col_data in metadata_columns.items() %}
  , {{ col_name }} {{ col_data.get('type') }}
  {%- endfor %}
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

{{ dbt_object_mgmt._execute(sql) }}
{%- endmacro -%}
