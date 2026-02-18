#!/usr/bin/env python3

import logging
import os
import sys
import pandas as pd
import requests


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)

file_handler = logging.FileHandler("iss.log")
file_handler.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
stream_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

logger.addHandler(stream_handler)
logger.addHandler(file_handler)


def extract():
    """Download and return the parsed JSON record from the ISS location API."""
    url = "http://api.open-notify.org/iss-now.json"

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        record = response.json()
        return record
    except requests.exceptions.RequestException as e:
        logger.error("Request failed: %s", e)
        return None
    except ValueError as e:
        logger.error("Failed to parse JSON: %s", e)
        return None


def transform(record):
    """Convert the JSON record into a single-row pandas DataFrame with a readable timestamp."""
    ts = record["timestamp"]
    dt = pd.to_datetime(ts, unit="s", utc=True)

    row = {
        "timestamp": dt.strftime("%Y-%m-%d %H:%M:%S UTC"),
        "latitude": float(record["iss_position"]["latitude"]),
        "longitude": float(record["iss_position"]["longitude"]),
    }

    row["datetime"] = dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    row["year"] = dt.year
    row["month"] = dt.month
    row["day"] = dt.day
    row["hour"] = dt.hour
    row["minute"] = dt.minute
    row["second"] = dt.second

    return pd.DataFrame([row])



def load(df, csv_file):
    """Append the row DataFrame to the CSV file, creating the file if it does not exist."""
    if os.path.exists(csv_file):
        existing = pd.read_csv(csv_file)
        combined = pd.concat([existing, df], ignore_index=True)
        combined.to_csv(csv_file, index=False)
    else:
        df.to_csv(csv_file, index=False)


def main():
    """Run the ETL pipeline: extract -> transform -> load."""
    if len(sys.argv) > 2:
        logger.error("Usage: python3 iss.py [output_csv]")
        return

    csv_file = sys.argv[1] if len(sys.argv) == 2 else "iss_output.csv"

    record = extract()
    if record is None:
        return

    df = transform(record)
    load(df, csv_file)


if __name__ == "__main__":
    main()