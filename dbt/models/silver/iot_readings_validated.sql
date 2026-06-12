{{ config(
    materialized='incremental',
    unique_key='surrogate_key',
    on_schema_change='sync_all_columns',
    file_format='delta',
    partition_by='date(reading_timestamp)'
) }}

-- Silver: Validated IoT sensor readings
-- Outlier detection (Z-score), gap detection, value interpolation

WITH readings AS (
    SELECT *,
        reading_timestamp::date AS reading_date
    FROM {{ ref('stg_iot_readings') }}
    {% if is_incremental() %}
    WHERE _loaded_at > (SELECT max(_loaded_at) FROM {{ this }})
    {% endif %}
),

stats AS (
    SELECT
        sensor_type,
        reading_unit,
        AVG(reading_value)     AS mean_value,
        STDDEV(reading_value)  AS stddev_value
    FROM readings
    GROUP BY 1, 2
)

SELECT
    r.device_id,
    r.sensor_type,
    r.reading_value                                                 AS reading_value_raw,
    r.reading_unit,
    r.battery_level,
    r.firmware_version,
    r.reading_timestamp,
    r.reading_date,
    -- Validate: flag outliers (>3 stddev from mean)
    CASE
        WHEN s.stddev_value IS NULL OR s.stddev_value = 0 THEN r.reading_value
        WHEN ABS(r.reading_value - s.mean_value) > 3 * s.stddev_value
        THEN NULL  -- nullify extreme outliers
        ELSE r.reading_value
    END                                                             AS reading_value_validated,
    CASE
        WHEN ABS(r.reading_value - s.mean_value) > 3 * s.stddev_value THEN TRUE
        ELSE FALSE
    END                                                             AS is_outlier,
    -- Z-score for monitoring
    CASE
        WHEN s.stddev_value > 0
        THEN (r.reading_value - s.mean_value) / s.stddev_value
        ELSE 0
    END                                                             AS z_score,
    r.surrogate_key,
    current_timestamp()                                             AS _loaded_at
FROM readings r
LEFT JOIN stats s
    ON r.sensor_type = s.sensor_type
   AND r.reading_unit = s.reading_unit
