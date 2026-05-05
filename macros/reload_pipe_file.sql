{%- macro reload_pipe_file(file, file_name) -%}

{{ dbt_object_mgmt._require_file(file, './snowflake/snowpipe/s3_pipe_jaffle_shop_customers.yml') }}

{%- set file_name = file_name or var('file_name') -%}
{%- set pipe = dbt_object_mgmt.gather_results(file) -%}
{%- set database_name = pipe.database_name or target.database %}
{%- set schema_name = database_name ~ '.' ~ pipe.schema_name %}
{%- set table_name = pipe.table_name %}
{%- set file_type = pipe.file_type %}

{%- set format_options = dbt_object_mgmt._build_format_options(file_type, pipe.extra_format_options) -%}

{%- set metadata_columns = dbt_object_mgmt._resolve_metadata_columns(pipe.custom_metadata_columns) -%}

{% set copy_statement -%}
  {{ dbt_object_mgmt._build_copy_statement(pipe, schema_name, metadata_columns, format_options, target_table=schema_name~'.'~table_name~'_temp', raw_pattern='$file_name') }}
{%- endset %}


{%- set sql -%}

-- Drop the file needing to be reloaded
delete from {{ schema_name }}.{{ table_name }}
  where file_name = '{{ file_name }}'
;

-- Create a temp table to load data
create or replace temporary table
  {{ schema_name }}.{{ table_name }}_temp
  like {{ schema_name }}.{{ table_name }}
;

-- Load file by name
{{ copy_statement }}
;

-- merge these back into the original table
insert into {{ schema_name }}.{{ table_name }}
  select * from {{ schema_name }}.{{ table_name }}_temp
;

{%- endset -%}

{{ dbt_object_mgmt._execute(sql) }}
{%- do log("Loaded " ~ file_name ~ " to table " ~ schema_name ~ "." ~ table_name, info=True) -%}

{%- endmacro -%}
