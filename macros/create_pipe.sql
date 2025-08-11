{%- macro create_pipe(file) -%}

{% if not file %}
  {{ exceptions.raise_compiler_error(
    "\nyou must pass in a file via arguments:" ~
    "\n  --args 'file: ./snowflake/snowpipe/s3_pipe_jaffle_shop_customers.yml'"
  ) }}
{% endif %}

{% set pipe = dbt_object_mgmt.gather_results(file) %}
{%- set database_name = pipe.database_name or target.database %}
{%- set schema_name = database_name ~ '.' ~ pipe.schema_name %}
{%- set table_name = pipe.table_name %}
{%- set file_type = pipe.file_type %}

{# set some defaults #}
{%- set format_type_options = {
    'skip_header': 1,
    'null_if': ('', 'null'),
    'error_on_column_count_mismatch': true,
    'field_optionally_enclosed_by': '\'"\'', 
  }
  if file_type == 'CSV'
  else {}
%}

{% if pipe.extra_format_options %}
  {{ format_type_options.update(pipe.extra_format_options) }}
{% endif %}

{% set metadata_columns = {
      'file_name':          {'source':'metadata$filename',           'type':'text',},
      'file_id':            {'source':'metadata$file_content_key',   'type':'text',},
      'row_number':         {'source':'metadata$file_row_number',    'type':'integer',},
      'last_modified_time': {'source':'metadata$file_last_modified', 'type':'timestamp_ntz',},
      'load_time':          {'source':'metadata$start_scan_time',    'type':'timestamp_ltz',},
  }
%}

{# Update with custom metadata column overrides #}
{% if pipe.custom_metadata_columns %}
  {% for col_name, config in pipe.custom_metadata_columns.items() %}
    {% if metadata_columns.get(col_name) %} {# If column already exists, update it #}
      {% if config is mapping %} {# If config is a dict (column: {source: ..., type: ...}) #}
          {% for key, value in config.items() %}
            {% do metadata_columns[col_name].update({key: value}) %}
          {% endfor %}
      {% else %} {# If config is just a string (column: source) #}
        {% do metadata_columns[col_name].update({'source': config}) %}
      {% endif %}
    {% else %} {# If column doesn't exist, add it #}
      {% do metadata_columns.update({col_name: config}) %}  
    {% endif %}
  {% endfor %}
{% endif %}


{% set copy_statement -%}
copy into {{ schema_name }}.{{ table_name }} from
{% if pipe.match_by_column_name %}
  {# if match_by_column_name, reccomended to set this in the pipe definition
    extra_format_options:
      parse_header: true
      skip_header: 0
      error_on_column_count_mismatch: false
  -#}
  @{{ schema_name }}.{{ table_name }}_stage
  {{ "match_by_column_name = " ~ pipe.match_by_column_name }}

  {% if pipe.match_by_column_name -%}
    include_metadata = (
      {% for key, value in metadata_columns.items() %}
        {{- key }} = {{ value }}{{ ', ' if not loop.last }}
      {% endfor %}
    )
  {%- endif %}

{% else %} (
    select
      {%- if file_type == 'JSON' %}
      parse_json($1)
      {%- else %}
      {%- for col in pipe.columns %}
      {{ ', ' if not loop.first }}${{ loop.index }}
      {%- endfor %}
      {%- endif %}
      {% for config in metadata_columns.values() -%}
      , {{ config.get('source') }}
      {%- endfor %}
    from
      @{{ schema_name }}.{{ table_name }}_stage
  )
  {% endif %}
  on_error = continue
  {{ "pattern = '" ~ pipe.pattern ~ "'" if pipe.pattern }}
  file_format = (
    type = '{{ file_type }}'
    {% for key, value in format_type_options.items() %}
      {{- key }} = {{ value }}
    {% endfor %}
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

{%- do log(sql, info=True) -%}
{% if not var('dry_run', False) %}
  {%- do run_query(sql) -%}
{% endif %}
{%- endmacro -%}
