with staging as (
    select * from {{ ref('stg_weather') }}
),

daily_aggregated as (
    select
        city_id,
        city_name,
        date,

        min(temperature_2m) as temperature_2m_min,
        max(temperature_2m) as temperature_2m_max,
        avg(temperature_2m) as temperature_2m_avg,

        min(apparent_temperature) as apparent_temperature_min,
        max(apparent_temperature) as apparent_temperature_max,
        avg(apparent_temperature) as apparent_temperature_avg,

        sum(precipitation) as precipitation_total,
        avg(surface_pressure) as surface_pressure_avg,
        avg(soil_temperature_7_to_28cm) as soil_temperature_7_to_28cm_avg,
        avg(soil_moisture_7_to_28cm) as soil_moisture_7_to_28cm_avg,

        count(*) as total_hours,
        sum(case when synthetic_anomaly_flag then 1 else 0 end) as anomaly_hours,
        max(ingestion_timestamp) as last_updated
    from staging
    group by city_id, city_name, date
)

select * from daily_aggregated
order by date, city_id
