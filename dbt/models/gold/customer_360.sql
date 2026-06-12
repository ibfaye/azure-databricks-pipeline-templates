{{ config(
    materialized='table',
    file_format='delta'
) }}

-- Gold: Customer 360 View
-- Single customer view with LTV, RFM segmentation, churn risk

WITH customers AS (
    SELECT * FROM {{ ref('customers_cleaned') }}
),

transactions AS (
    SELECT * FROM {{ ref('sales_transactions_cleaned') }}
),

rfm AS (
    SELECT
        t.customer_email_hashed,
        MAX(t.transaction_date)                             AS last_purchase_date,
        DATEDIFF('day', MAX(t.transaction_date), current_date()) AS recency_days,
        COUNT(DISTINCT t.transaction_id)                    AS frequency,
        SUM(t.amount_usd)                                   AS monetary,
        MIN(t.transaction_date)                             AS first_purchase_date
    FROM transactions t
    GROUP BY 1
)

SELECT
    c.customer_id,
    c.full_name_clean,
    c.email_hashed,
    c.country_code,
    c.registration_date,
    c.loyalty_tier,
    c.age_bucket,
    COALESCE(rfm.first_purchase_date, c.registration_date)  AS first_purchase_date,
    COALESCE(rfm.last_purchase_date, c.registration_date)   AS last_purchase_date,
    COALESCE(rfm.recency_days, 9999)                        AS recency_days,
    COALESCE(rfm.frequency, 0)                              AS lifetime_orders,
    COALESCE(rfm.monetary, 0)                               AS lifetime_value,
    -- RFM segments
    CASE
        WHEN rfm.monetary IS NULL THEN 'never_purchased'
        WHEN rfm.monetary > 1000 AND rfm.frequency > 10 AND rfm.recency_days < 30 THEN 'champion'
        WHEN rfm.monetary > 500  AND rfm.frequency > 5  AND rfm.recency_days < 60 THEN 'loyal'
        WHEN rfm.recency_days < 30 THEN 'new_active'
        WHEN rfm.recency_days BETWEEN 30 AND 90 THEN 'at_risk'
        WHEN rfm.recency_days > 90 THEN 'lapsed'
        ELSE 'needs_attention'
    END                                                     AS customer_segment,
    -- Churn risk score (0-100)
    CASE
        WHEN rfm.monetary IS NULL THEN 0
        WHEN rfm.recency_days > 90 THEN 90
        WHEN rfm.recency_days > 60 THEN 70
        WHEN rfm.recency_days > 30 THEN 40
        WHEN rfm.recency_days > 14 THEN 20
        ELSE 5
    END                                                     AS churn_risk_score,
    -- Average order value
    CASE
        WHEN rfm.frequency > 0 THEN rfm.monetary / rfm.frequency
        ELSE 0
    END                                                     AS avg_order_value,
    current_timestamp()                                     AS _loaded_at
FROM customers c
LEFT JOIN rfm ON c.email_hashed = rfm.customer_email_hashed
