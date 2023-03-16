{% macro get_qual_schema() %}
  {{ return(target.database~'.'~target.schema) }}
{% endmacro %}


{% macro get_stage_name() %}
  {{ return(get_qual_schema() ~ '.stage') }}
{% endmacro %}


{% macro get_file_format() %}
  {{ return(get_qual_schema() ~ '.file_format') }}
{% endmacro %}


{% macro put_file(file) %}

  {% set file_sql %}

    {% set stage = get_stage_name() %}

    create or replace stage {{ stage }};
    {{ log('created stage: ' ~ stage, info=True) }}

    put file://{{ file }} @{{ stage }};
    {{ log('put file: ' ~ file, info=True) }}

  {% endset %}

  {{ run_query(file_sql) }}

{% endmacro %}


{% macro create_file_format(file_args) %}

{% set file_sql %}
  create or replace file format {{ get_file_format() }}
  {{ file_args }}
  ;
{% endset %}

{{ run_query(file_sql) }}
{% endmacro %}

{% macro drop_objects() %}
  
  {{ run_query(file_sql) }}
  
{% endmacro %}
