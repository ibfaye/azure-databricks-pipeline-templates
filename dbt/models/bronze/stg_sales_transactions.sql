{{ config(materialized='view') }}

-- Bronze staging: Raw sales transactions
-- Source: POS/ERP system via ADLS landing zone

WITH source AS (
    SELECT * FROM {{ source('landing_sales', 'raw_sales_transactions') }}
)

SELECT
    transaction_id,
    store_id,
    product_sku,
    quantity,
    unit_price,
    amount,
    currency,
    payment_method,
    customer_email,
    transaction_date::timestamp AS transaction_date,
    -- Add surrogate key for downstream use
    {{ generate_surrogate_key(['transaction_id', 'transaction_date']) }} AS surrogate_key,
    -- Audit trail
    {{ add_audit_columns() }}
FROM source
WHERE transaction_date IS NOT NULL
