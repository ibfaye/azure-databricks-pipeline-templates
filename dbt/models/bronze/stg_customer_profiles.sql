{{ config(materialized='view') }}

-- Bronze staging: Raw customer profiles
-- Source: CRM system

WITH source AS (
    SELECT * FROM {{ source('landing', 'raw_customer_profiles') }}
)

SELECT
    customer_id,
    email,
    full_name,
    phone_number,
    date_of_birth,
    address_line1,
    address_city,
    address_country,
    registration_date::date AS registration_date,
    loyalty_tier,
    {{ generate_surrogate_key(['customer_id']) }} AS surrogate_key,
    {{ add_audit_columns() }}
FROM source
