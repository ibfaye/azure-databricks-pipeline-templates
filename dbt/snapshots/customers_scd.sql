{# SCD Type 2 snapshot for customer profiles.
   Tracks changes to customer data over time.
   Uncomment to activate. #}
{# 
{% snapshot customers_scd %}
  {{ config(
      target_schema='snapshots',
      unique_key='customer_id',
      strategy='timestamp',
      updated_at='_loaded_at',
      invalidate_hard_deletes=true
  ) }}
  SELECT * FROM {{ ref('customers_cleaned') }}
{% endsnapshot %}
#}
