with staging as (
    select * from {{ ref('stg_weather') }}
),

daily_aggregated as (
    select
        city_id,
        city_name,
        date,
        
        min(temperature_2m) as temp_min,
        max(temperature_2m) as temp_max,
        avg(temperature_2m) as temp_avg,
        stddev(temperature_2m) as temp_stddev,
        
        sum(precipitation) as precip_total,
        max(precipitation) as precip_max,
        count(case when precipitation > 0 then 1 end) as hours_with_rain,
        
        avg(wind_speed_10m) as wind_avg,
        max(wind_speed_10m) as wind_max,
        
        avg(relative_humidity_2m) as humidity_avg,
        avg(pressure_msl) as pressure_avg,
        avg(cloud_cover) as cloud_cover_avg,
        
        count(*) as total_hours,
        sum(has_missing_temp) as missing_temp_count,
        sum(has_missing_precip) as missing_precip_count,
        
        max(ingestion_timestamp) as last_updated
    from staging
    group by city_id, city_name, date
)

select * from daily_aggregated
order by date, city_id
