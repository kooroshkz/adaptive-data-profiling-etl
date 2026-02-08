# Expanded Weather Data Collection

## Overview
Expanded data collection from 6 hourly variables to **30 hourly + 20 daily variables** to capture comprehensive atmospheric, soil, and solar data.

## Changes Made

### 1. Data Collection (ingestion/config.py)
**Hourly Variables (30 total)**:
- Basic meteorology: temperature_2m, relative_humidity_2m, dew_point_2m, apparent_temperature
- Precipitation: precipitation (total), rain, snowfall, snow_depth, weather_code
- Pressure: pressure_msl, surface_pressure
- Cloud cover: cloud_cover (total), cloud_cover_low, cloud_cover_mid, cloud_cover_high
- Evapotranspiration: et0_fao_evapotranspiration, vapour_pressure_deficit
- Wind (2 heights): wind_speed_10m, wind_speed_100m, wind_direction_10m, wind_direction_100m, wind_gusts_10m
- Soil temperature (4 depths): 0-7cm, 7-28cm, 28-100cm, 100-255cm
- Soil moisture (4 depths): 0-7cm, 7-28cm, 28-100cm, 100-255cm

**Daily Variables (20 total)**:
- Temperature stats: temperature_2m_max, temperature_2m_min, temperature_2m_mean
- Apparent temperature: apparent_temperature_max, apparent_temperature_min, apparent_temperature_mean
- Solar: sunrise, sunset, daylight_duration, sunshine_duration, shortwave_radiation_sum
- Precipitation: precipitation_sum, rain_sum, snowfall_sum, precipitation_hours, weather_code
- Wind: wind_speed_10m_max, wind_gusts_10m_max, wind_direction_10m_dominant
- Evapotranspiration: et0_fao_evapotranspiration

### 2. Data Storage (weather_ingest.py)
**New Structure**:
- Hourly data: `{city}_hourly_{start}_{end}_{batch_id}.parquet`
- Daily data: `{city}_daily_{start}_{end}_{batch_id}.parquet`
- Each parquet file includes: weather variables + metadata (city_id, city_name, latitude, longitude, timezone, ingestion_timestamp, batch_id)

### 3. Database Schema (duckdb/setup_database.py)
**New Tables**:
- `raw.weather_hourly` - Hourly measurements (38 columns: 30 weather + 8 metadata)
- `raw.weather_daily` - Daily aggregates (28 columns: 20 weather + 8 metadata)
- `staging.weather_hourly` - Cleaned hourly data with date dimensions
- `mart.weather_daily` - Daily aggregations from hourly data
- `mart.weather_anomalies` - Z-score anomaly detection

### 4. dbt Models (weather_dbt/)
**Updated Source References**:
- Changed from `source('raw', 'weather')` to `source('raw', 'weather_hourly')`
- Added `weather_daily` source definition for future daily aggregation models
- All tests updated to reference new table structure

### 5. GitHub Actions (.github/workflows/daily-ingestion.yml)
**No Changes Needed**:
- Workflow automatically uploads all `.parquet` files regardless of naming convention
- Will now upload both `*_hourly_*.parquet` and `*_daily_*.parquet` files

## Test Results

### Ingestion Test (Amsterdam, Feb 8, 2026)
- Hourly file: 38 columns, 24 rows, 0.02 MB
- Daily file: 28 columns, 1 row, 0.02 MB
- API successfully returned all 50 requested variables

### Full Pipeline Test (5 cities, Feb 7, 2026)
- Hourly data: 144 rows (5 cities × 24 hours + 1 extra Amsterdam day)
- Daily data: 6 rows (5 cities + 1 extra)
- DuckDB tables: raw → staging → mart (all created successfully)
- dbt models: 3 models passed (stg_weather view + 2 mart tables)

### Data Quality
- All weather variables successfully captured
- No missing required fields
- Temperature range validation: -17°C (New York) to 12.6°C (Paris)
- All 5 cities present in final mart tables

## Next Steps

1. **Test Historical Backfill**: Run full 2-year backfill (2024-01-01 to 2025-12-31)
2. **Update dbt Models**: Add more sophisticated transformations using new variables:
   - Soil moisture analysis
   - Cloud cover correlation with temperature
   - Wind pattern analysis at multiple heights
   - Solar radiation and energy calculations
3. **Deploy to Cloud**: Update GitHub Actions to run with new configuration
4. **Airflow Integration**: Create DAG using expanded dataset (Step 5 - deferred)
5. **Documentation**: Update README with new variable list

## Cost Impact
- S3 storage: Approximately 2x increase (hourly + daily files)
- GitHub Actions: No change (still within free tier)
- Open-Meteo API: Still free tier (no rate limit issues observed)

## Technical Debt Cleanup
- Removed old `raw.weather` and `staging.weather` tables (kept for backward compatibility during migration)
- Can drop old tables once confirmed no dependencies exist