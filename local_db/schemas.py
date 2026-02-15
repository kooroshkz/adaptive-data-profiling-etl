"""DuckDB database schema definitions."""

RAW_WEATHER_SCHEMA = """
CREATE TABLE IF NOT EXISTS raw.weather (
    time TIMESTAMP,
    temperature_2m DOUBLE,
    relative_humidity_2m INTEGER,
    precipitation DOUBLE,
    wind_speed_10m DOUBLE,
    cloud_cover INTEGER,
    pressure_msl DOUBLE,
    city_id VARCHAR,
    city_name VARCHAR,
    latitude DOUBLE,
    longitude DOUBLE,
    timezone VARCHAR,
    ingestion_timestamp TIMESTAMP,
    batch_id VARCHAR
);
"""

STAGING_WEATHER_SCHEMA = """
CREATE TABLE IF NOT EXISTS staging.weather AS
SELECT
    time,
    CAST(time AS DATE) AS date,
    EXTRACT(year FROM time) AS year,
    EXTRACT(month FROM time) AS month,
    EXTRACT(day FROM time) AS day,
    EXTRACT(hour FROM time) AS hour,
    EXTRACT(dow FROM time) AS day_of_week,
    EXTRACT(quarter FROM time) AS quarter,
    
    COALESCE(temperature_2m, 0.0) AS temperature_2m,
    COALESCE(relative_humidity_2m, 0) AS relative_humidity_2m,
    COALESCE(precipitation, 0.0) AS precipitation,
    COALESCE(wind_speed_10m, 0.0) AS wind_speed_10m,
    COALESCE(cloud_cover, 0) AS cloud_cover,
    COALESCE(pressure_msl, 1013.25) AS pressure_msl,
    
    city_id,
    city_name,
    latitude,
    longitude,
    timezone,
    
    ingestion_timestamp,
    batch_id,
    
    CASE WHEN temperature_2m IS NULL THEN 1 ELSE 0 END AS has_missing_temp,
    CASE WHEN precipitation IS NULL THEN 1 ELSE 0 END AS has_missing_precip
FROM raw.weather;
"""

MART_WEATHER_DAILY_SCHEMA = """
CREATE TABLE IF NOT EXISTS mart.weather_daily AS
SELECT
    city_id,
    city_name,
    date,
    
    MIN(temperature_2m) AS temp_min,
    MAX(temperature_2m) AS temp_max,
    AVG(temperature_2m) AS temp_avg,
    STDDEV(temperature_2m) AS temp_stddev,
    
    SUM(precipitation) AS precip_total,
    MAX(precipitation) AS precip_max,
    COUNT(CASE WHEN precipitation > 0 THEN 1 END) AS hours_with_rain,
    
    AVG(wind_speed_10m) AS wind_avg,
    MAX(wind_speed_10m) AS wind_max,
    
    AVG(relative_humidity_2m) AS humidity_avg,
    AVG(pressure_msl) AS pressure_avg,
    AVG(cloud_cover) AS cloud_cover_avg,
    
    COUNT(*) AS total_hours,
    SUM(has_missing_temp) AS missing_temp_count,
    SUM(has_missing_precip) AS missing_precip_count,
    
    MAX(ingestion_timestamp) AS last_updated
FROM staging.weather
GROUP BY city_id, city_name, date
ORDER BY date, city_id;
"""

MART_WEATHER_ANOMALIES_SCHEMA = """
CREATE TABLE IF NOT EXISTS mart.weather_anomalies AS
WITH stats AS (
    SELECT
        city_id,
        AVG(temperature_2m) AS avg_temp,
        STDDEV(temperature_2m) AS stddev_temp,
        AVG(precipitation) AS avg_precip,
        STDDEV(precipitation) AS stddev_precip
    FROM staging.weather
    GROUP BY city_id
)
SELECT
    w.time,
    w.city_id,
    w.city_name,
    w.temperature_2m,
    w.precipitation,
    
    (w.temperature_2m - s.avg_temp) / NULLIF(s.stddev_temp, 0) AS temp_zscore,
    (w.precipitation - s.avg_precip) / NULLIF(s.stddev_precip, 0) AS precip_zscore,
    
    CASE 
        WHEN ABS((w.temperature_2m - s.avg_temp) / NULLIF(s.stddev_temp, 0)) > 3 THEN TRUE
        ELSE FALSE
    END AS is_temp_anomaly,
    
    CASE
        WHEN ABS((w.precipitation - s.avg_precip) / NULLIF(s.stddev_precip, 0)) > 3 THEN TRUE
        ELSE FALSE
    END AS is_precip_anomaly
    
FROM staging.weather w
JOIN stats s ON w.city_id = s.city_id
WHERE 
    (w.temperature_2m - s.avg_temp) / NULLIF(s.stddev_temp, 0) IS NOT NULL
ORDER BY w.time;
"""
