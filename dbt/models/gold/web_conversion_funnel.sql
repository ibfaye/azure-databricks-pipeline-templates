{{ config(
    materialized='table',
    file_format='delta',
    partition_by='funnel_date'
) }}

-- Gold: E-Commerce Conversion Funnel
-- Session → View Item → Add to Cart → Purchase funnel analysis

WITH sessions AS (
    SELECT * FROM {{ ref('web_sessions_enriched') }}
),

events AS (
    SELECT
        session_id,
        event_type,
        event_timestamp::date AS event_date
    FROM {{ ref('stg_web_events') }}
),

session_metrics AS (
    SELECT
        s.session_id,
        MIN(s.event_date)                                        AS funnel_date,
        s.country_code,
        s.device_type,
        -- Funnel stages
        MAX(CASE WHEN e.event_type = 'page_view'     THEN 1 ELSE 0 END) AS had_page_view,
        MAX(CASE WHEN e.event_type = 'product_view'  THEN 1 ELSE 0 END) AS had_product_view,
        MAX(CASE WHEN e.event_type = 'add_to_cart'   THEN 1 ELSE 0 END) AS had_add_to_cart,
        MAX(CASE WHEN e.event_type = 'checkout'      THEN 1 ELSE 0 END) AS had_checkout,
        MAX(CASE WHEN e.event_type = 'purchase'      THEN 1 ELSE 0 END) AS had_purchase
    FROM sessions s
    LEFT JOIN events e ON s.session_id = e.session_id
    GROUP BY 1, 2, 3, 4
)

SELECT
    funnel_date,
    country_code,
    device_type,
    COUNT(*)                                            AS sessions,
    SUM(had_page_view)                                  AS page_views,
    SUM(had_product_view)                               AS product_views,
    SUM(had_add_to_cart)                                AS added_to_cart,
    SUM(had_checkout)                                   AS checkouts,
    SUM(had_purchase)                                   AS purchases,
    -- Conversion rates
    ROUND(SUM(had_product_view) * 100.0 / NULLIF(COUNT(*), 0), 2)    AS view_rate_pct,
    ROUND(SUM(had_add_to_cart) * 100.0 / NULLIF(SUM(had_page_view), 0), 2)  AS cart_rate_pct,
    ROUND(SUM(had_purchase) * 100.0 / NULLIF(SUM(had_add_to_cart), 0), 2)   AS purchase_rate_pct,
    ROUND(SUM(had_purchase) * 100.0 / NULLIF(COUNT(*), 0), 2)        AS overall_conversion_pct,
    current_timestamp()                                  AS _loaded_at
FROM session_metrics
GROUP BY 1, 2, 3
