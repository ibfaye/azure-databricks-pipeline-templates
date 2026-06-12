{#
  Snapshot strategy: SCD Type 2 using dbt_valid_from/to.
  Timestamp-based with a surrogate key as the unique identifier.
#}
{% snapshot scd_type2 %}

{{
    config(
        target_schema='snapshots',
        unique_key='surrogate_key',
        strategy='timestamp',
        updated_at='loaded_at',
        invalidate_hard_deletes=True,
        file_format='delta'
    )
}}

{% endsnapshot %}
