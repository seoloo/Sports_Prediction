{#
  dbt 기본 동작은 "기본 스키마_사용자 스키마"(예: gold_staging)로 이름을 붙인다.
  이 프로젝트는 계층 이름을 그대로 스키마로 쓰기 위해(staging, gold) 사용자 스키마 이름을 그대로 돌려준다.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name | trim if custom_schema_name is not none else target.schema }}
{%- endmacro %}
