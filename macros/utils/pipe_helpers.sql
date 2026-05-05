{% macro _resolve_metadata_columns(custom_overrides=None) %}
  {%- set metadata_columns = {
      'file_name':          {'source': 'metadata$filename',           'type': 'text'},
      'file_id':            {'source': 'metadata$file_content_key',   'type': 'text'},
      'row_number':         {'source': 'metadata$file_row_number',    'type': 'integer'},
      'last_modified_time': {'source': 'metadata$file_last_modified', 'type': 'timestamp_ntz'},
      'load_time':          {'source': 'metadata$start_scan_time',    'type': 'timestamp_ltz'},
  } -%}

  {%- if custom_overrides -%}
    {%- for col_name, config in custom_overrides.items() -%}
      {%- if metadata_columns.get(col_name) -%}
        {%- if config is mapping -%}
          {%- for key, value in config.items() -%}
            {%- do metadata_columns[col_name].update({key: value}) -%}
          {%- endfor -%}
        {%- else -%}
          {%- do metadata_columns[col_name].update({'source': config}) -%}
        {%- endif -%}
      {%- else -%}
        {%- do metadata_columns.update({col_name: config}) -%}
      {%- endif -%}
    {%- endfor -%}
  {%- endif -%}

  {{ return(metadata_columns) }}
{% endmacro %}


{% macro _build_copy_select(columns, file_type, metadata_columns, stage) %}
(
    select
      {%- if file_type == 'JSON' %}
      parse_json($1)
      {%- else %}
      {%- for col in columns %}
      {{ ', ' if not loop.first }}${{ loop.index }}
      {%- endfor %}
      {%- endif %}
      {% for config in metadata_columns.values() -%}
      , {{ config.get('source') }}
      {%- endfor %}
    from
      @{{ stage }}
  )
{% endmacro %}


{% macro _build_file_format(file_type, format_options={}) %}
file_format = (
    type = '{{ file_type }}'
    {% for key, value in format_options.items() %}
      {{- key }} = {{ value }}
    {% endfor %}
  )
{% endmacro %}


{% macro _build_format_options(file_type, extra_format_options=none) %}
  {%- set format_options = {
      'skip_header': 1,
      'null_if': ('', 'null'),
      'error_on_column_count_mismatch': true,
      'field_optionally_enclosed_by': "'\"'"
    }
    if file_type == 'CSV'
    else {}
  -%}
  {%- if extra_format_options -%}
    {%- do format_options.update(extra_format_options) -%}
  {%- endif -%}
  {{ return(format_options) }}
{% endmacro %}


{% macro _build_copy_statement(pipe, schema_name, metadata_columns, format_options, target_table=none, raw_pattern=none) %}
{%- set table_name = pipe.table_name -%}
{%- set stage = schema_name ~ '.' ~ table_name ~ '_stage' -%}
{%- set dest = target_table if target_table is not none else schema_name ~ '.' ~ table_name -%}
copy into {{ dest }} from
{% if pipe.match_by_column_name %}
  {# if match_by_column_name, recommended to set in the pipe definition:
    extra_format_options:
      parse_header: true
      skip_header: 0
      error_on_column_count_mismatch: false
  -#}
  @{{ stage }}
  {{ "match_by_column_name = " ~ pipe.match_by_column_name }}

  {%- if pipe.match_by_column_name %}
  include_metadata = (
    {% for key, value in metadata_columns.items() %}
      {{- key }} = {{ value.get('source') }}{{ ', ' if not loop.last }}
    {% endfor %}
  )
  {%- endif %}

{% else %}
  {{ dbt_object_mgmt._build_copy_select(pipe.columns, pipe.file_type, metadata_columns, stage) }}
{% endif %}
  on_error = continue
  {%- if raw_pattern is not none %}
  pattern = {{ raw_pattern }}
  {%- elif pipe.pattern %}
  {{ "pattern = '" ~ pipe.pattern ~ "'" }}
  {%- endif %}
  {{ dbt_object_mgmt._build_file_format(pipe.file_type, format_options) }}
{% endmacro %}
