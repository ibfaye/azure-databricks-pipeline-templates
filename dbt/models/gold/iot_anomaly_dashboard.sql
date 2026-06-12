{{ config(
    materialized='table',
    file_format='delta',
    partition_by='report_date'
) }}

-- Gold: IoT Anomaly Dashboard
-- Device health summary, anomaly rates, battery health, gap detection

WITH readings AS (
    SELECT * FROM {{ ref('iot_readings_validated') }}
),

device_daily AS (
    SELECT
        device_id,
        sensor_type,
        reading_date,
        COUNT(*)                                                        AS readings_count,
        COUNT(CASE WHEN is_outlier THEN 1 END)                          AS outlier_count,
        AVG(reading_value_validated)                                    AS avg_reading,
        MIN(reading_value_validated)                                    AS min_reading,
        MAX(reading_value_validated)                                    AS max_reading,
        AVG(battery_level)                                              AS avg_battery_level,
        AVG(z_score)                                                    AS avg_z_score,
        MAX(reading_timestamp)                                          AS last_reading
    FROM readings
    GROUP BY 1, 2, 3
),

weekly_rollup AS (
    SELECT
        device_id,
        sensor_type,
        reading_date                                            AS report_date,
        readings_count,
        outlier_count,
        avg_reading,
        min_reading,
        max_reading,
        avg_battery_level,
        avg_z_score,
        last_reading,
        -- 7-day rolling anomaly rate
        SUM(outlier_count) OVER (
            PARTITION BY device_id, sensor_type
            ORDER BY reading_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) * 100.0 / NULLIF(SUM(readings_count) OVER (
            PARTITION BY device_id, sensor_type
            ORDER BY reading_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 0)                                                   AS anomaly_rate_7d_pct,
        -- Days since last reading
        DATEDIFF('day', LAG(last_reading) OVER (
            PARTITION BY device_id, sensor_type
            ORDER BY reading_date
        ), last_reading)                                        AS hours_since_last,
        current_timestamp()                                     AS _loaded_at
    FROM device_daily
)

SELECT * FROM weekly_rollup
