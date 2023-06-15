{%- macro reload_pipe_file(file) -%}

{% set file_name = var('file_name') %}

{% set pipe = gather_results(file) %}
{%- set schema_name = target.database ~ '.' ~ pipe.schema_name %}
{%- set table_name = pipe.table_name %}
{%- set file_type = pipe.file_type %}

{%- set format_type_options = set_type_options(
    pipe.extra_format_options or {}
  )
%}

{%- set sql -%}
begin;

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
copy into {{ schema_name }}.{{ table_name }}_temp
from (
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
pattern = '{{ file_name }}'
  file_format = (
    type = '{{ file_type }}'
    {% for key, value in format_type_options.items() %}
      {{- key }} = {{ value }}
    {% endfor -%}
    )
;

-- merge these back into the original table
insert into {{ schema_name }}.{{ table_name }}
  select * from {{ schema_name }}.{{ table_name }}_temp
;

commit;
{%- endset -%}


{% set message = "Loaded "~file_name~" to table "~schema_name~"."~table_name %}
{{ run_it(sql, message) }}

{%- endmacro -%}
