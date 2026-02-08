with staging as (
    select * from {{ ref('stg_weather') }}
),

stats as (
    select
        city_id,
        avg(temperature_2m) as avg_temp,
        stddev(temperature_2m) as stddev_temp,
        avg(precipitation) as avg_precip,
        stddev(precipitation) as stddev_precip
    from staging
    group by city_id
),

anomalies as (
    select
        s.time,
        s.city_id,
        s.city_name,
        s.temperature_2m,
        s.precipitation,
        
        (s.temperature_2m - st.avg_temp) / nullif(st.stddev_temp, 0) as temp_zscore,
        (s.precipitation - st.avg_precip) / nullif(st.stddev_precip, 0) as precip_zscore,
        
        case 
            when abs((s.temperature_2m - st.avg_temp) / nullif(st.stddev_temp, 0)) > 3 then true
            else false
        end as is_temp_anomaly,
        
        case
            when abs((s.precipitation - st.avg_precip) / nullif(st.stddev_precip, 0)) > 3 then true
            else false
        end as is_precip_anomaly
        
    from staging s
    join stats st on s.city_id = st.city_id
    where (s.temperature_2m - st.avg_temp) / nullif(st.stddev_temp, 0) is not null
)

select * from anomalies
order by time
