import json
from pathlib import Path
from datetime import datetime
import sys

import requests
import pandas as pd

# Command
# python src\weather_etl_date.py 2011-04-27

# --- Configuration ---

# Approximate coords (edit if needed)
LATITUDE = 31.8473
LONGITUDE = -87.52224

# Which hourly variables to pull
HOURLY_VARS = ["temperature_2m", "relativehumidity_2m"]

# Base output directories (relative to where you run the script)
RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")


def ensure_output_dirs():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def build_api_url(latitude: float, longitude: float,
                  start_date: str, end_date: str) -> str:
    """
    Build Open-Meteo archive API URL for a given date range.
    Dates must be in 'YYYY-MM-DD' format.
    """
    base_url = "https://archive-api.open-meteo.com/v1/archive"
    hourly = ",".join(HOURLY_VARS)

    return (
        f"{base_url}?latitude={latitude}&longitude={longitude}"
        f"&hourly={hourly}"
        f"&start_date={start_date}&end_date={end_date}"
        f"&timezone=auto"
    )


def extract_weather_data(latitude: float, longitude: float,
                         start_date: str, end_date: str) -> dict:
    url = build_api_url(latitude, longitude, start_date, end_date)
    print(f"Requesting: {url}")
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
    # --- Parse command line ---
    # Usage:
    #   python weather.py 2023-07-15
    #   python weather.py 2023-07-10 2023-07-15
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python weather.py YYYY-MM-DD")
        print("  python weather.py YYYY-MM-DD YYYY-MM-DD")
        sys.exit(1)

    start_date = sys.argv[1]
    if len(sys.argv) >= 3:
        end_date = sys.argv[2]
    else:
        end_date = start_date  # single day

    print(f"Fetching historical weather from {start_date} to {end_date}...")

    ensure_output_dirs()
    raw = extract_weather_data(LATITUDE, LONGITUDE, start_date, end_date)
    print("Transforming data...")
    df = transform_weather_data(raw)
    print(f"Transformed {len(df)} hourly records.")
    print("Writing outputs...")
    load_outputs(raw, df)
    print("Done.")


if __name__ == "__main__":
    main()
