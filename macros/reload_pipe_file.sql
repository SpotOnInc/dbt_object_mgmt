{%- macro reload_pipe_file(file, file_name) -%}

{{ dbt_object_mgmt._require_file(file, './snowflake/snowpipe/s3_pipe_jaffle_shop_customers.yml') }}

{%- set file_name = file_name or var('file_name') -%}
{%- set pipe = dbt_object_mgmt.gather_results(file) -%}
{%- set database_name = pipe.database_name or target.database %}
{%- set schema_name = database_name ~ '.' ~ pipe.schema_name %}
{%- set table_name = pipe.table_name %}
{%- set file_type = pipe.file_type %}

{%- set format_options = {
    'skip_header': 1,
    'null_if': ('', 'null'),
    'error_on_column_count_mismatch': true,
    'field_optionally_enclosed_by': '\'"\''
  }
  if file_type == 'CSV'
  else {}
%}
{% if pipe.extra_format_options %}
  {{ format_options.update(pipe.extra_format_options) }}
{% endif %}

{%- set metadata_columns = dbt_object_mgmt._resolve_metadata_columns(pipe.custom_metadata_columns) -%}

{% set copy_statement -%}
copy into {{ schema_name }}.{{ table_name }}_temp
{% if pipe.match_by_column_name %}
  from @{{ schema_name }}.{{ table_name }}_stage
  {{ "match_by_column_name = " ~ pipe.match_by_column_name }}
{% else %}
  from {{ dbt_object_mgmt._build_copy_select(pipe.columns, file_type, metadata_columns, stage) }}
{% endif %}
  on_error = continue
  pattern = $file_name
  {{ dbt_object_mgmt._build_file_format(file_type, format_options) }}
{%- endset %}


{%- set sql -%}

-- Drop the file needing to be reloaded
delete from {{ schema_name }}.{{ table_name }}
  where file_name ilike $file_name
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
