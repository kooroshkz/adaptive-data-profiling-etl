with source as (
    -- Read from Hive-partitioned S3 parquet files: city=amsterdam/hourly_*.parquet
    select * from read_parquet(
        's3://{{ env_var("S3_BUCKET", "weather-data-koorosh-thesis") }}/raw/city=*/hourly_*.parquet',
        hive_partitioning = true,
        union_by_name = true
    )
),

transformed as (
    select
        time,
        cast(time as date) as date,
        extract(year from time) as year,
        extract(month from time) as month,
        extract(day from time) as day,
        extract(hour from time) as hour,
        extract(dow from time) as day_of_week,
        extract(quarter from time) as quarter,
        
        temperature_2m,
        apparent_temperature,
        precipitation,
        surface_pressure,
        soil_temperature_7_to_28cm,
        soil_moisture_7_to_28cm,
        
        city_id,
        city_name,
        latitude,
        longitude,
        timezone,
        
        ingestion_timestamp,
        batch_id,
        coalesce(synthetic_anomaly_flag, false) as synthetic_anomaly_flag,
        synthetic_shift_pct,
        synthetic_anomaly_target_column,
        synthetic_original_value,
        synthetic_anomaly_batch_id,
        synthetic_anomaly_details_json,
        
        case when temperature_2m is null then 1 else 0 end as has_missing_temp,
        case when precipitation is null then 1 else 0 end as has_missing_precip
    from source
)

select * from transformed
