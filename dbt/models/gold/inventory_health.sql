{{ config(
    materialized='table',
    file_format='delta',
    partition_by='snapshot_date'
) }}

-- Gold: Inventory Health Dashboard
-- ABC classification, days-on-hand, stockout risk, turnover

WITH inventory AS (
    SELECT * FROM {{ ref('inventory_snapshots') }}
),

daily_sales AS (
    SELECT
        product_sku,
        transaction_date::date AS sale_date,
        SUM(quantity)          AS daily_quantity_sold
    FROM {{ ref('sales_transactions_cleaned') }}
    GROUP BY 1, 2
),

moving_average AS (
    SELECT
        product_sku,
        AVG(daily_quantity_sold) OVER (
            PARTITION BY product_sku
            ORDER BY sale_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS avg_daily_demand_30d
    FROM daily_sales
),

latest_inventory AS (
    SELECT
        product_sku,
        warehouse_id,
        snapshot_date,
        quantity_on_hand,
        inbound_qty,
        outbound_qty,
        ROW_NUMBER() OVER (PARTITION BY product_sku ORDER BY snapshot_date DESC) AS rn
    FROM inventory
    WHERE quantity_on_hand >= 0
)

SELECT
    li.snapshot_date,
    li.product_sku,
    li.warehouse_id,
    li.quantity_on_hand,
    li.inbound_qty,
    li.outbound_qty,
    COALESCE(ma.avg_daily_demand_30d, 0) AS avg_daily_demand_30d,
    -- Days on hand (at current demand rate)
    CASE
        WHEN ma.avg_daily_demand_30d > 0
        THEN li.quantity_on_hand / ma.avg_daily_demand_30d
        ELSE 999
    END                                   AS days_on_hand,
    -- Stockout risk
    CASE
        WHEN li.quantity_on_hand = 0 THEN 'stocked_out'
        WHEN ma.avg_daily_demand_30d > 0 AND (li.quantity_on_hand / ma.avg_daily_demand_30d) < 7 THEN 'critical'
        WHEN ma.avg_daily_demand_30d > 0 AND (li.quantity_on_hand / ma.avg_daily_demand_30d) < 30 THEN 'low'
        WHEN ma.avg_daily_demand_30d > 0 AND (li.quantity_on_hand / ma.avg_daily_demand_30d) < 90 THEN 'healthy'
        ELSE 'overstocked'
    END                                   AS stockout_risk,
    -- ABC classification (Pareto — based on demand volume)
    CASE
        WHEN ma.avg_daily_demand_30d IS NULL OR ma.avg_daily_demand_30d = 0 THEN 'C'
        WHEN ma.avg_daily_demand_30d > 50 THEN 'A'
        WHEN ma.avg_daily_demand_30d > 10 THEN 'B'
        ELSE 'C'
    END                                   AS abc_class,
    current_timestamp()                   AS _loaded_at
FROM latest_inventory li
LEFT JOIN (
    SELECT DISTINCT product_sku, first_value(avg_daily_demand_30d) OVER w AS avg_daily_demand_30d
    FROM moving_average
    WINDOW w AS (PARTITION BY product_sku ORDER BY sale_date DESC)
) ma ON li.product_sku = ma.product_sku
WHERE li.rn = 1
