"""
Web scraper for real-time air quality data from airquality.am
Downloads and reads the latest hourly sensor data CSV file.
"""
import requests
import pandas as pd
import numpy as np
from datetime import datetime
import time
import io
from pathlib import Path

class AirQualityScraper:
    """
    Downloads and parses the latest sensor_avg_hourly CSV file.
    """

    def __init__(self):
        self.base_url = "https://airquality.am/data/sensor_avg_hourly/"
        self.current_year = datetime.now().year
        self.cached_data = None
        self.cache_time = None
        self.cache_duration = 300  # 5 minutes in seconds

    def get_latest_file_url(self):
        """
        Get the URL for the most recent sensor_avg_hourly CSV file.
        """
        return f"{self.base_url}sensor_avg_hourly_{self.current_year}.csv"

    def download_latest_data(self):
        """
        Download the latest sensor_avg_hourly CSV file.
        """
        url = self.get_latest_file_url()

        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }

            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()

            # Read CSV data
            # Note: First line is "SET", second line is header
            csv_data = response.text
            lines = csv_data.split('\n')

            # Skip the first line ("SET") and use second line as header
            if len(lines) > 1 and lines[0].strip() == "SET":
                header = lines[1].strip().split(',')
                data_lines = lines[2:]
                csv_content = '\n'.join([','.join(header)] + data_lines)
            else:
                csv_content = csv_data

            # Read into pandas DataFrame
            df = pd.read_csv(io.StringIO(csv_content))

            # Parse timestamp
            if 'timestamp' in df.columns:
                df['datetime'] = pd.to_datetime(df['timestamp'])

            # Ensure PM2.5 column exists
            pm25_col = None
            for col in ['pm2.5', 'pm2.5_corrected', 'pm25']:
                if col in df.columns:
                    pm25_col = col
                    break

            if pm25_col and pm25_col != 'pm25':
                df['pm25'] = df[pm25_col]

            self.cached_data = df
            self.cache_time = time.time()

            return df

        except requests.RequestException as e:
            print(f"  Error downloading data: {e}")
            return None
        except Exception as e:
            print(f"  Error parsing data: {e}")
            return None

    def get_current_readings(self, force_refresh=False):
        """
        Get current readings for all sensors.
        Uses cached data if available and not expired.
        """
        current_time = time.time()

        # Check cache
        if not force_refresh and self.cached_data is not None and self.cache_time:
            if current_time - self.cache_time < self.cache_duration:
                return self._extract_latest_readings()

        # Download fresh data
        df = self.download_latest_data()
        if df is None:
            return []

        return self._extract_latest_readings()

    def _extract_latest_readings(self):
        """
        Extract the most recent reading for each sensor.
        """
        if self.cached_data is None:
            return []

        df = self.cached_data

        # Get the latest timestamp
        if 'datetime' in df.columns:
            latest_time = df['datetime'].max()
            latest_df = df[df['datetime'] == latest_time]
        else:
            # If no datetime, assume data is already latest
            latest_df = df

        readings = []
        for _, row in latest_df.iterrows():
            reading = {
                'sensor_id': row.get('sensor_id'),
                'pm25': row.get('pm25'),
                'timestamp': row.get('datetime') if 'datetime' in row else datetime.now(),
                'source': 'CSV download',
                'file_time': datetime.now().strftime('%Y-%m-%d %H:%M')
            }

            # Add other available columns
            for col in ['temperature', 'humidity', 'pressure']:
                if col in row:
                    reading[col] = row[col]

            readings.append(reading)

        return readings

    def get_sensor_reading(self, sensor_id):
        """
        Get current reading for a specific sensor.
        """
        readings = self.get_current_readings()

        for reading in readings:
            if reading.get('sensor_id') == sensor_id:
                return reading

        return None

    def get_all_readings_dict(self):
        """
        Get all readings as a dictionary keyed by sensor_id.
        """
        readings = self.get_current_readings()
        result = {}
        for r in readings:
            if r.get('sensor_id'):
                result[r['sensor_id']] = r
        return result

    def get_recent_history(self, sensor_id, hours=24):
        """
        Get recent historical data for a specific sensor.
        """
        if self.cached_data is None:
            self.get_current_readings()

        if self.cached_data is None:
            return pd.DataFrame()

        df = self.cached_data

        if 'sensor_id' not in df.columns:
            return pd.DataFrame()

        sensor_df = df[df['sensor_id'] == sensor_id].copy()

        if len(sensor_df) == 0:
            return pd.DataFrame()

        if 'datetime' in sensor_df.columns:
            sensor_df = sensor_df.sort_values('datetime', ascending=False)
            return sensor_df.head(hours)
        else:
            return sensor_df.head(hours)

# Simple test if run directly
if __name__ == "__main__":
    scraper = AirQualityScraper()

    print("Downloading latest sensor data...")
    readings = scraper.get_current_readings()

    print(f"\nFound {len(readings)} sensors with current data")

    if readings:
        print("\nSample readings:")
        for r in readings[:5]:
            print(f"  Sensor {r['sensor_id']}: {r['pm25']} µg/m³")

    # Test specific sensor
    sensor_41 = scraper.get_sensor_reading(41)
    if sensor_41:
        print(f"\nSensor 41: {sensor_41}")