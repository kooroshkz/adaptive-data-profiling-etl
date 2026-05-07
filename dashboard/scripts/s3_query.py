#!/usr/bin/env python3
"""Query weather parquet data in S3 using DuckDB and return JSON for dashboard APIs."""

import argparse
import json
import os
from pathlib import Path
from typing import Any

import duckdb

CITIES = ["amsterdam", "new_york", "london", "paris", "tokyo"]
DAILY_DATASETS = {"daily_with_anomalies", "daily_without_anomalies"}
DATASET_PATHS = {
    "raw_hourly": "raw/city=*/hourly_*.parquet",
}

# Numeric columns available in the hourly parquet schema
_NUMERIC_COLS = {
    "temperature_2m", "apparent_temperature", "precipitation",
    "surface_pressure", "soil_temperature_7_to_28cm", "soil_moisture_7_to_28cm",
    "relative_humidity_2m", "dew_point_2m", "rain", "snowfall", "snow_depth",
    "weather_code", "pressure_msl", "cloud_cover", "cloud_cover_low",
    "cloud_cover_mid", "cloud_cover_high", "et0_fao_evapotranspiration",
    "vapour_pressure_deficit", "wind_speed_10m", "wind_speed_100m",
    "wind_direction_10m", "wind_direction_100m", "wind_gusts_10m",
    "soil_temperature_0_to_7cm", "soil_temperature_28_to_100cm",
    "soil_temperature_100_to_255cm", "soil_moisture_0_to_7cm",
    "soil_moisture_28_to_100cm", "soil_moisture_100_to_255cm",
}


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _load_env_fallback() -> None:
    """Load credentials from airflow/.env when not already present in process env."""
    repo_root = Path(__file__).resolve().parents[2]
    env_path = repo_root / "airflow" / ".env"
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and os.getenv(key) is None:
            os.environ[key] = value


def _connect() -> duckdb.DuckDBPyConnection:
    _load_env_fallback()
    con = duckdb.connect(":memory:")
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")

    aws_region = os.getenv("AWS_REGION", "eu-west-1")
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    con.execute(f"SET s3_region = {_sql_literal(aws_region)}")
    if aws_access_key_id:
        con.execute(f"SET s3_access_key_id = {_sql_literal(aws_access_key_id)}")
    if aws_secret_access_key:
        con.execute(f"SET s3_secret_access_key = {_sql_literal(aws_secret_access_key)}")

    return con


def _local_raw_parquet_glob() -> str:
    """Absolute glob path for all hourly parquets in the local mirror."""
    repo_root = Path(__file__).resolve().parents[2]
    return str(repo_root / "airflow" / "data" / "raw" / "city=*" / "hourly_*.parquet")


def _local_daily_scatter(dataset: str, city: str, y_col: str, limit: int) -> dict[str, Any]:
    """Compute daily aggregates from local parquets without MotherDuck."""
    if y_col not in _NUMERIC_COLS:
        raise ValueError(f"Column not supported for local daily aggregation: {y_col}")

    glob = _local_raw_parquet_glob()
    city_filter = f"AND city_id = '{city}'" if city else ""
    include_anomalies = dataset == "daily_with_anomalies"

    anomaly_filter = "" if include_anomalies else (
        "AND (synthetic_anomaly_flag IS NULL OR NOT synthetic_anomaly_flag)"
    )

    limit_clause = f"LIMIT {int(limit)}" if limit and int(limit) > 0 else ""

    sql = f"""
        SELECT
            DATE_TRUNC('day', time)                                              AS day_ts,
            city_id,
            epoch(DATE_TRUNC('day', time)) * 1000.0                             AS x_value,
            AVG({y_col})                                                         AS y_value,
            COALESCE(BOOL_OR(synthetic_anomaly_flag), FALSE)                    AS has_anomaly,
            COUNT(*) FILTER (WHERE synthetic_anomaly_flag)                       AS anomaly_hours
        FROM read_parquet({repr(glob)}, union_by_name=true, hive_partitioning=true)
        WHERE time IS NOT NULL
          AND {y_col} IS NOT NULL
          {anomaly_filter}
          {city_filter}
        GROUP BY DATE_TRUNC('day', time), city_id
        ORDER BY day_ts DESC
        {limit_clause}
    """

    con = duckdb.connect(":memory:")
    rows = con.execute(sql).fetchall()

    points = []
    anomaly_count = 0
    for row in rows:
        day_ts, city_id, x_value, y_value, has_anomaly, anom_hours = row
        is_anomaly = bool(has_anomaly) and include_anomalies
        if is_anomaly:
            anomaly_count += 1
        points.append({
            "time": _serialize_value(day_ts),
            "cityId": city_id,
            "x": float(x_value),
            "y": float(y_value) if y_value is not None else 0.0,
            "isAnomaly": is_anomaly,
            "isSyntheticAny": bool(has_anomaly),
            "shiftPct": None,
            "actualValue": None,
            "targetColumn": None,
            "anomalyHours": int(anom_hours) if anom_hours else 0,
        })

    return {
        "dataset": dataset,
        "city": city,
        "xColumn": "time",
        "yColumn": y_col,
        "rowCount": len(points),
        "anomalyCount": anomaly_count,
        "totalSyntheticCount": sum(1 for p in points if p["isSyntheticAny"]),
        "anomalyRate": (anomaly_count / len(points)) if points else 0,
        "avgShiftPct": None,
        "points": points,
    }


def _connect_motherduck() -> duckdb.DuckDBPyConnection:
    _load_env_fallback()
    token = os.getenv("MOTHERDUCK_TOKEN")
    if not token:
        raise RuntimeError("MOTHERDUCK_TOKEN is required for daily aggregate datasets")

    con = duckdb.connect(f"md:?motherduck_token={token}")
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")

    aws_region = os.getenv("AWS_REGION", "eu-west-1")
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    con.execute(f"SET s3_region = {_sql_literal(aws_region)}")
    if aws_access_key_id:
        con.execute(f"SET s3_access_key_id = {_sql_literal(aws_access_key_id)}")
    if aws_secret_access_key:
        con.execute(f"SET s3_secret_access_key = {_sql_literal(aws_secret_access_key)}")

    return con


def _dataset_sql(dataset: str, city: str = "") -> str:
    if dataset in DATASET_PATHS:
        bucket = os.getenv("S3_BUCKET", "weather-data-koorosh-thesis")
        parquet_path = DATASET_PATHS[dataset]
        # Raw parquet files can evolve over time (e.g., synthetic columns in backfill only).
        # union_by_name=true allows reading mixed schemas and fills missing columns with NULL.
        return (
            f"read_parquet('s3://{bucket}/{parquet_path}', "
            "hive_partitioning=true, union_by_name=true)"
        )

    if dataset in DAILY_DATASETS:
        if not city:
            raise ValueError("Daily datasets require a city")
        if city not in CITIES:
            raise ValueError(f"Unknown city: {city}")
        view_name = f"weather_analytics.main.weather_{dataset}_{city}"
        return view_name

    raise ValueError(f"Unsupported dataset: {dataset}")


def _serialize_value(value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def get_metadata(dataset: str) -> dict[str, Any]:
    if dataset in DAILY_DATASETS:
        con = _connect_motherduck()
        source_sql = _dataset_sql(dataset, CITIES[0])
        cities = CITIES
    else:
        con = _connect()
        source_sql = _dataset_sql(dataset)
        city_rows = con.execute(
            f"SELECT DISTINCT city_id FROM {source_sql} WHERE city_id IS NOT NULL ORDER BY city_id"
        ).fetchall()
        cities = [row[0] for row in city_rows]

    schema_rows = con.execute(f"DESCRIBE SELECT * FROM {source_sql}").fetchall()
    columns = [{"name": row[0], "type": row[1]} for row in schema_rows]

    numeric_prefixes = (
        "BIGINT",
        "INTEGER",
        "SMALLINT",
        "TINYINT",
        "HUGEINT",
        "UBIGINT",
        "UINTEGER",
        "USMALLINT",
        "UTINYINT",
        "FLOAT",
        "DOUBLE",
        "DECIMAL",
        "REAL",
    )

    numeric_columns = [
        col["name"]
        for col in columns
        if str(col["type"]).upper().startswith(numeric_prefixes)
    ]

    has_anomaly_flag = any(col["name"] == "synthetic_anomaly_flag" for col in columns)

    return {
        "dataset": dataset,
        "cities": cities,
        "columns": columns,
        "numericColumns": numeric_columns,
        "hasSyntheticAnomalyFlag": has_anomaly_flag,
    }


def get_scatter_data(dataset: str, city: str, y_col: str, limit: int) -> dict[str, Any]:
    is_daily_dataset = dataset in DAILY_DATASETS

    # Daily aggregates are always computed from local parquets (ground truth)
    if is_daily_dataset:
        return _local_daily_scatter(dataset, city, y_col, limit)

    con = _connect()
    source_sql = _dataset_sql(dataset, city)

    metadata = get_metadata(dataset)
    valid_columns = {col["name"] for col in metadata["columns"]}

    if y_col not in valid_columns:
        raise ValueError(f"Invalid y column: {y_col}")
    if city and city not in metadata["cities"]:
        raise ValueError(f"Unknown city: {city}")

    select_parts = [
        "time",
        "city_id",
        "epoch(time) * 1000.0 AS x_value",
        f"{y_col} AS y_value",
    ]

    if "synthetic_anomaly_flag" in valid_columns:
        select_parts.append("synthetic_anomaly_flag")
    else:
        select_parts.append("FALSE AS synthetic_anomaly_flag")

    if "anomaly_hours" in valid_columns:
        select_parts.append("anomaly_hours")
    else:
        select_parts.append("NULL AS anomaly_hours")

    if "synthetic_shift_pct" in valid_columns:
        select_parts.append("synthetic_shift_pct")
    else:
        select_parts.append("NULL AS synthetic_shift_pct")

    if "synthetic_anomaly_target_column" in valid_columns:
        select_parts.append("synthetic_anomaly_target_column")
    else:
        select_parts.append("NULL AS synthetic_anomaly_target_column")

    if "synthetic_anomaly_details_json" in valid_columns:
        select_parts.append("synthetic_anomaly_details_json")
    else:
        select_parts.append("NULL AS synthetic_anomaly_details_json")

    if limit and int(limit) > 0:
        query = f"""
            SELECT {', '.join(select_parts)}
            FROM {source_sql}
            WHERE (? = '' OR city_id = ?)
                        AND time IS NOT NULL
              AND {y_col} IS NOT NULL
            ORDER BY time DESC
            LIMIT ?
        """
        rows = con.execute(query, [city, city, int(limit)]).fetchall()
    else:
        query = f"""
            SELECT {', '.join(select_parts)}
            FROM {source_sql}
            WHERE (? = '' OR city_id = ?)
                        AND time IS NOT NULL
              AND {y_col} IS NOT NULL
            ORDER BY time DESC
        """
        rows = con.execute(query, [city, city]).fetchall()

    point_rows = []
    anomaly_count = 0
    total_synthetic_count = 0
    shift_values = []

    for row in rows:
        is_synthetic_any = bool(row[4])
        anomaly_hours = row[5]
        point_shift = None
        actual_value = None
        target_col = row[7]
        details_json = row[8]

        if is_daily_dataset:
            is_anomaly = is_synthetic_any or (anomaly_hours is not None and float(anomaly_hours) > 0)
        else:
            details_map = None
            if details_json:
                try:
                    details_map = json.loads(details_json)
                except json.JSONDecodeError:
                    details_map = None

            selected_detail = None
            if isinstance(details_map, dict):
                selected_detail = details_map.get(y_col)

            is_anomaly = is_synthetic_any and (
                (target_col == y_col) or (isinstance(selected_detail, dict))
            )

            if isinstance(selected_detail, dict) and selected_detail.get("shift_pct") is not None:
                point_shift = float(selected_detail["shift_pct"])
                if selected_detail.get("actual") is not None:
                    actual_value = float(selected_detail["actual"])
            elif row[6] is not None:
                point_shift = float(row[6])

        if is_synthetic_any:
            total_synthetic_count += 1

        if is_anomaly:
            anomaly_count += 1
            if not is_daily_dataset:
                if point_shift is not None:
                    shift_values.append(float(point_shift))

        point_rows.append(
            {
                "time": _serialize_value(row[0]),
                "cityId": row[1],
                "x": float(row[2]),
                "y": float(row[3]),
                "isAnomaly": is_anomaly,
                "isSyntheticAny": is_synthetic_any,
                "shiftPct": point_shift,
                "actualValue": actual_value,
                "targetColumn": target_col,
                "anomalyHours": None if anomaly_hours is None else int(anomaly_hours),
            }
        )

    avg_shift_pct = (sum(shift_values) / len(shift_values)) if shift_values else None

    return {
        "dataset": dataset,
        "city": city,
        "xColumn": "time",
        "yColumn": y_col,
        "rowCount": len(point_rows),
        "anomalyCount": anomaly_count,
        "totalSyntheticCount": total_synthetic_count,
        "anomalyRate": (anomaly_count / len(point_rows)) if point_rows else 0,
        "avgShiftPct": avg_shift_pct,
        "points": point_rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Query weather S3 parquet data")
    parser.add_argument("--action", choices=["metadata", "scatter"], required=True)
    parser.add_argument("--dataset", default="raw_hourly")
    parser.add_argument("--city", default="")
    parser.add_argument("--y-column", default="precipitation")
    parser.add_argument("--limit", type=int, default=0)

    args = parser.parse_args()

    if args.action == "metadata":
        payload = get_metadata(args.dataset)
    else:
        payload = get_scatter_data(
            dataset=args.dataset,
            city=args.city,
            y_col=args.y_column,
            limit=args.limit,
        )

    print(json.dumps(payload, ensure_ascii=True))


if __name__ == "__main__":
    main()
