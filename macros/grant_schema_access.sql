{% macro grant_schema_access(roles, resource_types=["model", "seed"]) %}

{% if execute %}

{% if roles is not string and roles is not iterable %}
  {% do exceptions.raise_compiler_error('"roles" must be a string or a list') %}
{% elif roles is string %}
  {% set roles = [roles] %}
{% endif %}

{% set models = graph.nodes.values() | selectattr("resource_type", "in", resource_types) | list %}
{% set schemas = [] %}

{% for model in models %}
  {% set qualified_schema = model.database.lower()~'.'~model.schema.lower() %}
  {{ schemas.append(qualified_schema) if qualified_schema not in schemas }}
{% endfor %}


{% set information_query %}
  select lower(catalog_name || '.' || schema_name) as database_schema
  from {{ target.database }}.information_schema.schemata
  where lower(database_schema) in (
    {{ "\'" + schemas | join("\', \'") + "\'" }}
  )
{% endset %}
{%- set snowflake_schemas = dbt_utils.get_query_results_as_dict(information_query).get('DATABASE_SCHEMA') -%}

{% for schema in schemas if schema in snowflake_schemas %}
  {% for role in roles %}
    {% set schema_2_role = schema~' to role '~role %}

    {{ log("Granting: "~schema~' -> '~role, info=True) }}

    {{ run_query('grant usage on schema '~schema_2_role) }}
    {{ run_query('grant select on all tables in schema '~schema_2_role) }}
    {{ run_query('grant select on all views in schema '~schema_2_role) }}

  {% endfor %}
{% endfor %}

{% endif %}

{% endmacro %}
