{% macro generate_schema_name(custom_schema, node) -%}
  {%- if target.name == 'prod' -%}
    {{ custom_schema }}
  {%- else -%}
    {{ target.schema ~ '_' ~ custom_schema }}
  {%- endif -%}
{%- endmacro %}
