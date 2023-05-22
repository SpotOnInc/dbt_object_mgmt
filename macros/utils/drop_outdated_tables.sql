{% macro drop_outdated_tables(schema, excluded_tables='') %}
  {% if (schema is not string and schema is not iterable) or schema is mapping or schema|length <= 0 %}
    {% do exceptions.raise_compiler_error('"schema" must be a string or a list') %}
  {% elif schema is string %}
    {% set schema = [schema] %}
  {% endif %}

  {%- set model_sql -%}
    with snowflake_objects as (
      select
          table_catalog
          , table_schema
          , table_name
          , 'view' as type
      from {{ target.database }}.information_schema.views
      where lower(table_schema) in (
          {% for s in schema -%}
            {{ ", " if not loop.first }}'{{ s }}'
          {% endfor -%}
      )
      union all
      select
        table_catalog
        , table_schema
        , table_name
        , 'table' as type
      from {{ target.database }}.information_schema.tables
      where table_name not in {{ excluded_tables }}
        and lower(table_schema) in (
        {% for s in schema -%}
          {{ ", " if not loop.first }}'{{ s }}'
        {% endfor -%}
      )
    )
    , dbt_nodes as (
      select
        table_schema
        , table_name
      from (
        values
        {%- set tables = graph.nodes.values() | selectattr("resource_type", "equalto", "model") | list -%}
        {%- set seeds = graph.nodes.values() | selectattr("resource_type", "equalto", "seed") | list -%}
        {%- for node in tables + seeds | list %}
          {{ ", " if not loop.first }}('{{ node.schema }}', '{{ node.alias }}')
        {%- endfor %}
      ) as this (table_schema, table_name)
    )

    select
      snow.table_catalog
      , snow.table_schema
      , snow.table_name
      , snow.type
    from
      snowflake_objects as snow
    left join dbt_nodes as models
      on lower(snow.table_schema) = models.table_schema
      and lower(snow.table_name) = models.table_name
    where models.table_name is null
  {%- endset %}

  {{ log(model_sql, info=True) }}

  {% set results = run_query(model_sql) %}

  {%- for table_catalog, table_schema, table_name, table_type in results %}

    {% set snow_obj = '.'.join([table_catalog, table_schema, table_name]) %}

    {% set drop_query -%}
      drop {{ table_type }} if exists {{ snow_obj }} cascade;
    {% endset %}

    {% if not var('dry_run', false) %}
      {% do log('dropped ' ~ table_type ~ ': ' ~ snow_obj, info=true) %}
      {% do run_query(drop_query) %}
    {% else %}
      {% do log('would drop ' ~ table_type ~ ': ' ~ snow_obj, info=true) %}
    {% endif %}

  {%- endfor %}

{% endmacro %}
