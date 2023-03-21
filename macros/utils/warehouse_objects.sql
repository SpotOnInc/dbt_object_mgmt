{% macro get_qual_schema() %}
  {{ return(target.database~'.'~target.schema) }}
{% endmacro %}


{% macro get_stage_name() %}
  {{ return(get_qual_schema() ~ '.stage') }}
{% endmacro %}


{% macro get_file_format() %}
  {{ return(get_qual_schema() ~ '.file_format') }}
{% endmacro %}


{% macro get_file_type(file) %}
  {{ return(file.split('.')[-1]) }}
{% endmacro %}


{% macro validate_filetype(file) %}
  {% set accepted_types = ['yml', 'yaml', 'json'] %}
  {% set file_type = get_file_type(file) %}

  {% if file_type not in accepted_types %}
    {% do exceptions.raise_compiler_error(
    '\nfile format needs to be in ' ~ ', '.join(accepted_types) ~
    ' not ' ~ file_type
  ) %}
  {% endif %}
{% endmacro %}


{% macro put_file(file) %}

  {{ validate_filetype(file) }}

  {% set put_sql %}

    {% set stage = get_stage_name() %}

    create or replace stage {{ stage }};
    {{ log('created stage: ' ~ stage, info=True) }}

    put file://{{ file }} @{{ stage }};
    {{ log('put file: ' ~ file, info=True) }}

  {% endset %}

  {{ run_query(put_sql) }}

{% endmacro %}


{% macro create_file_format(file) %}

  {% set file_type = get_file_type(file) %}

  {% set format_args %}
    {% if file_type == 'json' %}
      type = 'JSON'
    {% else %}
      type = 'CSV'
      record_delimiter = '||'
    {% endif %}
  {% endset %}

  {% set format_sql %}
    create or replace file format {{ get_file_format() }}
    {{ format_args }}
    ;
  {% endset %}

  {{ run_query(format_sql) }}

{% endmacro %}


{% macro gather_results(file) %}

  {{ put_file(file) }}

  {{ create_file_format(file) }}

  {% set data_sql %}
    select
      $1 as raw_data
    from @{{ get_stage_name() }} (
      file_format => {{ get_file_format() }}
    )
    ;
  {% endset %}

  {% set results = run_query(data_sql)[0][0] %}

  {{ return(results) }}
{% endmacro %}
