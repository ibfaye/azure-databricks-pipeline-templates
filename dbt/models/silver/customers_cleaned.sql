{{ config(
    materialized='incremental',
    unique_key='customer_id',
    on_schema_change='sync_all_columns',
    file_format='delta'
) }}

-- Silver: Cleansed customer profiles
-- Standardize names, validate emails, mask PII

WITH source AS (
    SELECT * FROM {{ ref('stg_customer_profiles') }}
),

cleaned AS (
    SELECT
        customer_id,
        -- Standardize names: title case, trim whitespace
        initcap(trim(full_name))                    AS full_name_clean,
        -- Validate email format
        CASE
            WHEN email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
            THEN {{ mask_pii('email', 'hash') }}
            ELSE NULL
        END                                         AS email_hashed,
        -- Mask phone
        {{ mask_pii('phone_number', 'mask_phone') }} AS phone_masked,
        date_of_birth,
        address_city,
        upper(address_country)                       AS country_code,
        registration_date,
        loyalty_tier,
        surrogate_key,
        _loaded_at
    FROM source
)

SELECT
    customer_id,
    full_name_clean,
    email_hashed,
    phone_masked,
    date_of_birth,
    address_city,
    country_code,
    registration_date,
    loyalty_tier,
    -- Age bucket for analytics
    CASE
        WHEN date_of_birth IS NULL THEN 'unknown'
        WHEN months_between(current_date(), date_of_birth) / 12 < 18 THEN 'under_18'
        WHEN months_between(current_date(), date_of_birth) / 12 BETWEEN 18 AND 24 THEN '18_24'
        WHEN months_between(current_date(), date_of_birth) / 12 BETWEEN 25 AND 34 THEN '25_34'
        WHEN months_between(current_date(), date_of_birth) / 12 BETWEEN 35 AND 44 THEN '35_44'
        WHEN months_between(current_date(), date_of_birth) / 12 BETWEEN 45 AND 54 THEN '45_54'
        WHEN months_between(current_date(), date_of_birth) / 12 BETWEEN 55 AND 64 THEN '55_64'
        ELSE '65_plus'
    END AS age_bucket,
    surrogate_key,
    _loaded_at
FROM cleaned
