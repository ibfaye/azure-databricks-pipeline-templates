{{ config(
    materialized='table',
    file_format='delta',
    partition_by='sales_date'
) }}

-- Gold: Daily Sales Summary
-- Aggregated KPIs: revenue, orders, AOV by store/date/currency

WITH transactions AS (
    SELECT * FROM {{ ref('sales_transactions_cleaned') }}
)

SELECT
    transaction_date::date               AS sales_date,
    store_id,
    currency,
    COUNT(DISTINCT transaction_id)       AS order_count,
    COUNT(DISTINCT customer_email_hashed) AS unique_customers,
    SUM(amount_local)                    AS total_revenue_local,
    SUM(amount_usd)                      AS total_revenue_usd,
    AVG(amount_local)                    AS avg_order_value_local,
    AVG(amount_usd)                      AS avg_order_value_usd,
    SUM(quantity)                        AS items_sold,
    -- MoM growth flag
    CASE
        WHEN LAG(SUM(amount_usd)) OVER (
            PARTITION BY store_id, currency
            ORDER BY transaction_date::date
        ) > 0
        THEN (SUM(amount_usd) - LAG(SUM(amount_usd)) OVER (
            PARTITION BY store_id, currency
            ORDER BY transaction_date::date
        )) / LAG(SUM(amount_usd)) OVER (
            PARTITION BY store_id, currency
            ORDER BY transaction_date::date
        )
        ELSE NULL
    END                                 AS mom_revenue_growth,
    current_timestamp()                  AS _loaded_at
FROM transactions
GROUP BY 1, 2, 3
