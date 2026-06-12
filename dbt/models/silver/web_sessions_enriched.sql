{{ config(
    materialized='incremental',
    unique_key='session_id',
    on_schema_change='sync_all_columns',
    file_format='delta',
    partition_by='date(event_date)'
) }}

-- Silver: Enriched web sessions
-- Sessionize raw events, filter bots, extract UTM attribution

WITH events AS (
    SELECT
        session_id,
        user_id,
        event_type,
        page_url,
        referrer_url,
        device_type,
        browser,
        country_code,
        event_timestamp,
        event_timestamp::date AS event_date
    FROM {{ ref('stg_web_events') }}
    {% if is_incremental() %}
    WHERE _loaded_at > (SELECT max(_loaded_at) FROM {{ this }})
    {% endif %}
),

sessions AS (
    SELECT
        session_id,
        user_id,
        MIN(event_timestamp)                                                   AS session_start,
        MAX(event_timestamp)                                                   AS session_end,
        DATEDIFF('second', MIN(event_timestamp), MAX(event_timestamp))        AS session_duration_seconds,
        COUNT(DISTINCT CASE WHEN event_type = 'page_view' THEN page_url END)  AS pages_viewed,
        COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN page_url END)   AS purchases,
        COUNT(DISTINCT event_type)                                            AS distinct_event_types,
        FIRST(device_type)                                                     AS device_type,
        FIRST(browser)                                                         AS browser,
        FIRST(country_code)                                                    AS country_code,
        FIRST(referrer_url)                                                    AS landing_referrer,
        MIN(event_date)                                                        AS event_date,
        -- Bot detection: sessions with >50 page views/second or no user agent
        CASE
            WHEN session_duration_seconds = 0 AND COUNT(*) > 50 THEN TRUE
            WHEN session_duration_seconds > 0 AND (COUNT(*) / NULLIF(session_duration_seconds, 0)) > 10 THEN TRUE
            WHEN browser IS NULL THEN TRUE
            ELSE FALSE
        END                                                                    AS is_bot
    FROM events
    GROUP BY 1, 2
)

SELECT
    s.*,
    current_timestamp() AS _loaded_at
FROM sessions s
WHERE NOT is_bot
