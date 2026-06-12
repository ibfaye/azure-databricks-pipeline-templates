{{ config(materialized='view') }}

-- Bronze staging: Raw web analytics events
-- Source: Clickstream collector

WITH source AS (
    SELECT * FROM {{ source('landing', 'raw_web_events') }}
)

SELECT
    event_id,
    session_id,
    user_id,
    event_type,          -- page_view, click, add_to_cart, purchase, search
    page_url,
    referrer_url,
    device_type,
    browser,
    country_code,
    event_timestamp::timestamp AS event_timestamp,
    -- Extract UTM parameters
    parse_url(page_url, 'QUERY') AS page_query_string,
    {{ generate_surrogate_key(['event_id', 'event_timestamp']) }} AS surrogate_key,
    {{ add_audit_columns() }}
FROM source
