import json
from pathlib import Path
from datetime import datetime

import requests
import pandas as pd

# --- Configuration ---

# Approximate coords for Toronto, ON
LATITUDE = 43.65
LONGITUDE = -79.38

# Which hourly variables to pull
HOURLY_VARS = ["temperature_2m", "relativehumidity_2m"]

# Base output directories
RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")


def ensure_output_dirs():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def build_api_url(latitude: float, longitude: float) -> str:
    base_url = "https://api.open-meteo.com/v1/forecast"
    hourly = ",".join(HOURLY_VARS)
    # 1-day forecast, auto timezone
    return (
        f"{base_url}?latitude={latitude}&longitude={longitude}"
        f"&hourly={hourly}&forecast_days=1&timezone=auto"
    )


def extract_weather_data(latitude: float, longitude: float) -> dict:
    url = build_api_url(latitude, longitude)
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()


def transform_weather_data(raw: dict) -> pd.DataFrame:
    """
    Transform Open-Meteo hourly data into a tidy table.
    """
    hourly = raw.get("hourly", {})
    times = hourly.get("time", [])

    data = {"time": times}

    for var in HOURLY_VARS:
        data[var] = hourly.get(var, [])

    df = pd.DataFrame(data)

    # Convert time column to datetime
    df["time"] = pd.to_datetime(df["time"], errors="coerce")

    # Drop any empty time rows
    df = df.dropna(subset=["time"])

    return df


def load_outputs(raw: dict, df: pd.DataFrame) -> None:
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    raw_path = RAW_DIR / f"weather_raw_{timestamp}.json"
    csv_path = PROCESSED_DIR / f"weather_hourly_{timestamp}.csv"

    # Save raw JSON
    with raw_path.open("w", encoding="utf-8") as f:
        json.dump(raw, f, ensure_ascii=False, indent=2)

    # Save processed CSV
    df.to_csv(csv_path, index=False)

    print(f"Saved raw JSON to:      {raw_path}")
    print(f"Saved processed CSV to: {csv_path}")


def main():
    ensure_output_dirs()
    print("Fetching weather data from Open-Meteo...")
    raw = extract_weather_data(LATITUDE, LONGITUDE)
    print("Transforming data...")
    df = transform_weather_data(raw)
    print(f"Transformed {len(df)} hourly records.")
    print("Writing outputs...")
    load_outputs(raw, df)
    print("Done.")


if __name__ == "__main__":
    main()
