{#
  Unity Catalog schema generator.
  Maps dbt custom schemas to Unity Catalog {catalog}.{schema} format.
  Returns: catalog_name.schema_name (e.g., "bronze.sales")
#}
{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {# Tag-based catalog routing #}
        {%- if node.tags and 'bronze' in node.tags -%}
            {{ var('bronze_catalog', 'bronze') }}.{{ custom_schema_name | trim }}
        {%- elif node.tags and 'silver' in node.tags -%}
            {{ var('silver_catalog', 'silver') }}.{{ custom_schema_name | trim }}
        {%- elif node.tags and 'gold' in node.tags -%}
            {{ var('gold_catalog', 'gold') }}.{{ custom_schema_name | trim }}
        {%- else -%}
            {{ custom_schema_name | trim }}
        {%- endif -%}
    {%- endif -%}

{%- endmacro %}


{#
  Sets Unity Catalog context at the start of a dbt run.
  Ensures all models land in the correct catalog.
#}
{% macro set_uc_catalogs() %}
    {% set catalogs = [
        var('bronze_catalog', 'bronze'),
        var('silver_catalog', 'silver'),
        var('gold_catalog', 'gold')
    ] %}
    {% for catalog in catalogs %}
        {% set set_catalog_query %}
            USE CATALOG {{ catalog }};
        {% endset %}
        {% do run_query(set_catalog_query) %}
    {% endfor %}
{% endmacro %}


{#
  Incremental strategy selector.
  Uses merge for Unity Catalog (supports schema evolution).
  Falls back to append for append-only sources.
#}
{% macro get_incremental_strategy(strategy='merge') %}
    {% if strategy == 'merge' %}
        {{ return('merge') }}
    {% elif strategy == 'append' %}
        {{ return('append') }}
    {% else %}
        {{ return('merge') }}
    {% endif %}
{% endmacro %}


{#
  Surrogate key: generates a stable hash key from column values.
  Uses SHA2 (256-bit) for collision resistance.
#}
{% macro generate_surrogate_key(field_list) %}
    sha2(concat_ws('|',
        {%- for field in field_list -%}
            coalesce(cast({{ field }} as string), '_null_')
            {%- if not loop.last %}, {% endif -%}
        {%- endfor -%}
    ), 256)
{% endmacro %}


{#
  Data masking — hashes sensitive PII columns.
  Keeps original for bronze; silver masks; gold never sees PII.
#}
{% macro mask_pii(column_name, strategy='hash') %}
    {% set salt = env_var('DBT_PII_SALT', 'databricks-pii-salt-2025') %}
    {% if strategy == 'hash' %}
        sha2(concat_ws('|', '{{ salt }}', {{ column_name }}), 256)
    {% elif strategy == 'null' %}
        null
    {% elif strategy == 'mask_email' %}
        regexp_replace({{ column_name }}, '(.{2}).*(@.*)', '$1***$2')
    {% elif strategy == 'mask_phone' %}
        regexp_replace({{ column_name }}, '(\d{3})\d{6}(\d*)', '$1******$2')
    {% else %}
        {{ column_name }}
    {% endif %}
{% endmacro %}


{#
  Audit columns — adds standard metadata to every model.
#}
{% macro add_audit_columns() %}
    current_timestamp() as _loaded_at,
    current_user() as _loaded_by,
    current_version() as _databricks_runtime_version
{% endmacro %}


{#
  Run results logger — logs pass/fail/skip counts at end of dbt run.
#}
{% macro log_results(results) %}
    {% set passed = results | selectattr('status', 'equalto', 'success') | list | length %}
    {% set failed = results | selectattr('status', 'equalto', 'error') | list | length %}
    {% set skipped = results | selectattr('status', 'equalto', 'skipped') | list | length %}
    {{ log("=" * 60, info=True) }}
    {{ log("DBT RUN COMPLETE — Passed: " ~ passed ~ " | Failed: " ~ failed ~ " | Skipped: " ~ skipped, info=True) }}
    {{ log("=" * 60, info=True) }}
{% endmacro %}
