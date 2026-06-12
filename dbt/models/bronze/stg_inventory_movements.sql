{{ config(materialized='view') }}

-- Bronze staging: Raw inventory movements
-- Source: Warehouse management system

WITH source AS (
    SELECT * FROM {{ source('landing', 'raw_inventory_movements') }}
)

SELECT
    movement_id,
    product_sku,
    warehouse_id,
    quantity,
    movement_type,       -- INBOUND, OUTBOUND, TRANSFER, ADJUSTMENT
    from_location,
    to_location,
    movement_date::timestamp AS movement_date,
    reference_order_id,
    {{ generate_surrogate_key(['movement_id']) }} AS surrogate_key,
    {{ add_audit_columns() }}
FROM source
