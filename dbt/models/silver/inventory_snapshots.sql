{{ config(
    materialized='incremental',
    unique_key='surrogate_key',
    on_schema_change='sync_all_columns',
    file_format='delta',
    partition_by='snapshot_date'
) }}

-- Silver: Daily inventory position snapshots
-- Dedup movements, compute daily running balance

WITH movements AS (
    SELECT
        product_sku,
        warehouse_id,
        movement_type,
        quantity,
        movement_date::date AS movement_date,
        ROW_NUMBER() OVER (
            PARTITION BY movement_id
            ORDER BY _loaded_at DESC
        ) AS row_num
    FROM {{ ref('stg_inventory_movements') }}
),

deduped AS (
    SELECT * FROM movements WHERE row_num = 1
),

-- Compute net daily quantity change per product/warehouse
daily_flows AS (
    SELECT
        movement_date,
        product_sku,
        warehouse_id,
        SUM(CASE WHEN movement_type IN ('INBOUND', 'ADJUSTMENT+') THEN quantity ELSE 0 END) AS inbound_qty,
        SUM(CASE WHEN movement_type IN ('OUTBOUND', 'ADJUSTMENT-') THEN quantity ELSE 0 END) AS outbound_qty,
        SUM(CASE WHEN movement_type = 'TRANSFER' THEN quantity ELSE 0 END)         AS transfer_qty
    FROM deduped
    GROUP BY 1, 2, 3
)

SELECT
    movement_date                                   AS snapshot_date,
    product_sku,
    warehouse_id,
    -- Running total across dates
    SUM(inbound_qty - outbound_qty) OVER (
        PARTITION BY product_sku, warehouse_id
        ORDER BY movement_date
        ROWS UNBOUNDED PRECEDING
    )                                               AS quantity_on_hand,
    inbound_qty,
    outbound_qty,
    transfer_qty,
    {{ generate_surrogate_key(['product_sku', 'warehouse_id', 'movement_date']) }} AS surrogate_key,
    current_timestamp()                             AS _loaded_at
FROM daily_flows
