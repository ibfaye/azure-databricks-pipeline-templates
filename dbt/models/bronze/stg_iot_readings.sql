{{ config(materialized='view') }}

-- Bronze staging: Raw IoT sensor telemetry
-- Source: IoT hub -> ADLS streaming

WITH source AS (
    SELECT * FROM {{ source('landing_iot', 'raw_iot_sensor_readings') }}
)

SELECT
    device_id,
    sensor_type,          -- temperature, humidity, pressure, vibration, energy
    reading_value,
    reading_unit,
    battery_level,
    firmware_version,
    reading_timestamp::timestamp AS reading_timestamp,
    {{ generate_surrogate_key(['device_id', 'reading_timestamp', 'sensor_type']) }} AS surrogate_key,
    {{ add_audit_columns() }}
FROM source
