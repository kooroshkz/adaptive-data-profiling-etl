with source as (
    select * from {{ source('raw', 'weather_hourly') }}
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
        
        coalesce(temperature_2m, 0.0) as temperature_2m,
        coalesce(relative_humidity_2m, 0) as relative_humidity_2m,
        coalesce(precipitation, 0.0) as precipitation,
        coalesce(wind_speed_10m, 0.0) as wind_speed_10m,
        coalesce(cloud_cover, 0) as cloud_cover,
        coalesce(pressure_msl, 1013.25) as pressure_msl,
        
        city_id,
        city_name,
        latitude,
        longitude,
        timezone,
        
        ingestion_timestamp,
        batch_id,
        
        case when temperature_2m is null then 1 else 0 end as has_missing_temp,
        case when precipitation is null then 1 else 0 end as has_missing_precip
    from source
)

select * from transformed
