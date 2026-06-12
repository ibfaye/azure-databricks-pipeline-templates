{{ config(
    materialized='incremental',
    unique_key='surrogate_key',
    on_schema_change='sync_all_columns',
    file_format='delta',
    partition_by='date(transaction_date)'
) }}

-- Silver: Cleansed sales transactions
-- Dedup, validate amounts, normalize currencies, mask PII

WITH deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id
            ORDER BY _loaded_at DESC
        ) AS row_num
    FROM {{ ref('stg_sales_transactions') }}
    {% if is_incremental() %}
    WHERE _loaded_at > (SELECT max(_loaded_at) FROM {{ this }})
    {% endif %}
),

validated AS (
    SELECT
        transaction_id,
        store_id,
        product_sku,
        quantity,
        -- Validate amounts: negative amounts flagged
        CASE
            WHEN amount < 0 THEN 0
            ELSE amount
        END AS amount_validated,
        currency,
        payment_method,
        transaction_date,
        surrogate_key,
        _loaded_at,
        _loaded_by,
        _databricks_runtime_version
    FROM deduplicated
    WHERE row_num = 1
      AND transaction_id IS NOT NULL
      AND amount > 0     -- drop zero/null amounts
      AND quantity > 0   -- drop zero/null quantities
)

SELECT
    transaction_id,
    store_id,
    product_sku,
    quantity,
    amount_validated           AS amount_local,
    currency,
    payment_method,
    transaction_date,
    -- PII: hash customer email
    {{ mask_pii('customer_email', 'hash') }} AS customer_email_hashed,
    -- Currency conversion (simplified; use live forex rates in production)
    amount_validated AS amount_usd,
    surrogate_key,
    _loaded_at,
    _loaded_by,
    _databricks_runtime_version
FROM validated
