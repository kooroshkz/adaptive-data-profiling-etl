# Upstream Data Source: Open-Meteo Weather API

**Open-Meteo API** used as the upstream data source for weather data. It provides weather information for any location on Earth without requiring an API key. The API is free for non-commercial use and returns data in JSON format. 

The project uses two parts of this API, **Forecast / Past Days API** from the main docs and **Historical Weather API** for older past data.


## Base API

### What it Does

Provides weather forecast for up to 16 days, archived weather for recent days using the `past_days` parameter, Hourly and daily weather variables when provided with a location (latitude and longitude) and date range.

- **Historical Weather API**: Retrieving weather data from the distant past (many years back) with hourly and daily measurements.
    - **Weather data includes**:
        - Temperature and humidity
        - Precipitation and rain
        - Wind speed and direction
        - Cloud cover and pressure
        - Soil temperature and moisture
        - Many more variables

The API returns the historical weather data for a given time range and location in JSON format.

---

## API Usage

To query either endpoint:

```url

https://api.open-meteo.com/v1/forecast?
latitude={lat}&longitude={lon}&
start_date={YYYY-MM-DD}&end_date={YYYY-MM-DD}&
hourly={variables}&daily={variables}

```

For the historical API:

```url

https://archive-api.open-meteo.com/v1/archive?
latitude={lat}&longitude={lon}&
start_date={YYYY-MM-DD}&end_date={YYYY-MM-DD}&
hourly={variables}&daily={variables}

```

Example Output:

```JSON
{
    "latitude": 52.52,
    "longitude": 13.419,
    "elevation": 44.812,
    "generationtime_ms": 2.2119,
    "utc_offset_seconds": 0,
    "timezone": "Europe/Berlin",
    "timezone_abbreviation": "CEST",
    "hourly": {
        "time": ["2022-07-01T00:00", "2022-07-01T01:00", "2022-07-01T02:00", ...],
        "temperature_2m": [13, 12.7, 12.7, 12.5, 12.5, 12.8, 13, 12.9, 13.3, ...]
    },
    "hourly_units": {
        "temperature_2m": "°C"
    }
}
```

## Links to Documentation

- [Open-Meteo API Documentation](https://open-meteo.com/en/docs)
- [Historical Weather API Documentation](https://open-meteo.com/en/docs/historical-weather-api)