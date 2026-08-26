# Data source: Open-Meteo

Weather data comes from the free, keyless [Open-Meteo API](https://open-meteo.com/en/docs).
Two endpoints are used:

- Historical: `https://archive-api.open-meteo.com/v1/archive` (past data)
- Forecast: `https://api.open-meteo.com/v1/forecast` (recent days)

## Request

```
?latitude={lat}&longitude={lon}&start_date={YYYY-MM-DD}&end_date={YYYY-MM-DD}&hourly={vars}
```

Hourly variables used: `temperature_2m`, `apparent_temperature`, `precipitation`,
`surface_pressure`, `soil_temperature_7_to_28cm`, `soil_moisture_7_to_28cm`.

## Response (excerpt)

```json
{
  "latitude": 52.52, "longitude": 13.42, "timezone": "Europe/Berlin",
  "hourly": {
    "time": ["2022-07-01T00:00", "2022-07-01T01:00"],
    "temperature_2m": [13.0, 12.7]
  },
  "hourly_units": {"temperature_2m": "°C"}
}
```

Docs: [API](https://open-meteo.com/en/docs),
[Historical](https://open-meteo.com/en/docs/historical-weather-api).
