#!/usr/bin/env python3

from datetime import datetime
import logging
import os
import sys
import pandas as pd
import requests
import mysql.connector


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


def register_reporter(db, table, reporter_id, reporter_name):
    """Register the reporter if reporter_id is not already in the table."""
    cursor = None

    try:
        cursor = db.cursor()

        check_query = f"SELECT reporter_id FROM {table} WHERE reporter_id = %s"
        cursor.execute(check_query, (reporter_id,))
        result = cursor.fetchone()

        if result is None:
            insert_query = f"""
            INSERT INTO {table} (reporter_id, reporter_name)
            VALUES (%s, %s)
            """
            cursor.execute(insert_query, (reporter_id, reporter_name))
            db.commit()
            logger.info("Reporter added.")
        else:
            logger.info("Reporter already exists. Skipping insert.")

    except mysql.connector.Error as e:
        logger.error("Database error in register_reporter: %s", e)

    finally:
        if cursor is not None:
            cursor.close()


def load(db, table, record, reporter_id):
    """Insert the latest ISS location into the locations table."""
    cursor = None

    try:
        cursor = db.cursor()

        message = record["message"]
        latitude = record["iss_position"]["latitude"]
        longitude = record["iss_position"]["longitude"]
        timestamp = datetime.fromtimestamp(record["timestamp"]).strftime("%Y-%m-%d %H:%M:%S")

        insert_query = f"""
        INSERT INTO {table} (message, latitude, longitude, timestamp, reporter_id)
        VALUES (%s, %s, %s, %s, %s)
        """

        values = (message, latitude, longitude, timestamp, reporter_id)
        cursor.execute(insert_query, values)
        db.commit()

        logger.info("ISS location inserted.")

    except mysql.connector.Error as e:
        logger.error("Database error in load: %s", e)

    finally:
        if cursor is not None:
            cursor.close()


def main():
    """Run the ETL pipeline: extract -> transform -> load into MySQL."""
    db = None

    reporter_id = "bae7kx"
    reporter_name = "Kriti Shelke"

    try:
        db = mysql.connector.connect(
            host="ds2002.cgls84scuy1e.us-east-1.rds.amazonaws.com",
            user="ds2002",
            password="Xf3$fa57CwD!",
            database="iss"
        )

        register_reporter(db, "reporters", reporter_id, reporter_name)

        record = extract()
        if record is None:
            return

        df = transform(record)
        logger.info("\n%s", df)

        load(db, "locations", record, reporter_id)

    except mysql.connector.Error as e:
        logger.error("Database connection error: %s", e)

    finally:
        if db is not None:
            db.close()
            logger.info("Database connection closed.")


if __name__ == "__main__":
    main() 