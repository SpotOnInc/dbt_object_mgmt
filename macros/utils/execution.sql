{% macro _require_file(file, example_path='') %}
  {% if not file %}
    {{ exceptions.raise_compiler_error(
      "\nyou must pass in a file via arguments:" ~
      ("\n  --args 'file: " ~ example_path ~ "'" if example_path else "")
    ) }}
  {% endif %}
{% endmacro %}


{% macro _execute(sql) %}
  {%- do log(sql, info=True) -%}
  {% if not var('dry_run', False) %}
    {%- do run_query(sql) -%}
  {% endif %}
{% endmacro %}
