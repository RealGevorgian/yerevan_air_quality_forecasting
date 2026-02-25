"""
Fixed data loading module that handles problematic CSV files.
"""
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional, Union, List, Dict, Tuple
import logging
from datetime import datetime, timedelta
import glob
import os

logger = logging.getLogger(__name__)


class AirQualityDataLoaderFixed:
    """Fixed data loader that handles problematic CSV files."""

    def __init__(self, data_dir: Union[str, Path]):
        self.data_dir = Path(data_dir)
        self.measurements_dir = self.data_dir / "measurements"
        self.sensors_file = self.data_dir / "sensors.csv"
        self.sensors_df = None
        logger.info(f"Fixed data loader initialized with directory: {self.data_dir}")

    def load_sensors_metadata(self) -> pd.DataFrame:
        """Load sensor metadata with proper column mapping."""

        if not self.sensors_file.exists():
            raise FileNotFoundError(f"Sensors file not found: {self.sensors_file}")

        logger.info(f"Loading sensors metadata from {self.sensors_file}")
        df = pd.read_csv(self.sensors_file)

        # Map columns to expected names
        column_mapping = {
            'id': 'sensor_id',
            'station_id': 'station_id',
            'latitude': 'latitude',
            'longitude': 'longitude',
            'altitude': 'altitude',
            'sensor_type': 'sensor_type',
            'title': 'title',
            'provider': 'provider'
        }

        df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})

        self.sensors_df = df
        logger.info(f"Loaded {len(df)} sensors")
        return df

    def load_measurements_file_safe(self,
                                    file_path: Union[str, Path],
                                    nrows: Optional[int] = None) -> pd.DataFrame:

        file_path = Path(file_path)
        logger.info(f"Loading {file_path.name}")

        # Try different approaches
        approaches = [
            # Approach 1: Standard read
            lambda: pd.read_csv(file_path, nrows=nrows),

            # Approach 2: Skip bad lines
            lambda: pd.read_csv(file_path, nrows=nrows, on_bad_lines='skip'),

            # Approach 3: Python engine with bad lines skip
            lambda: pd.read_csv(file_path, nrows=nrows, engine='python', on_bad_lines='skip'),

            # Approach 4: No header, then assign columns
            lambda: pd.read_csv(file_path, nrows=nrows, header=None, on_bad_lines='skip'),

            # Approach 5: Low memory mode
            lambda: pd.read_csv(file_path, nrows=nrows, low_memory=False, on_bad_lines='skip'),
        ]

        for i, approach in enumerate(approaches, 1):
            try:
                df = approach()
                logger.info(f"  Approach {i} succeeded: {len(df)} rows")

                # If we used no header, try to infer header from first row
                if i == 4 and len(df) > 0:
                    # Check if first row might be header
                    first_row = df.iloc[0].astype(str)
                    if all(col.startswith(('date', 'sensor', 'pm', 'time')) for col in first_row):
                        df.columns = first_row
                        df = df.iloc[1:].reset_index(drop=True)
                    else:
                        # Assign generic column names
                        df.columns = [f'col_{i}' for i in range(len(df.columns))]

                return df

            except Exception as e:
                logger.debug(f"  Approach {i} failed: {e}")
                continue

        raise ValueError(f"All approaches failed for {file_path}")

    def load_measurements_range(self,
                                start_year: int,
                                end_year: Optional[int] = None,
                                months: Optional[List[int]] = None,
                                nrows_per_file: Optional[int] = None) -> pd.DataFrame:
        """
        Load measurements with robust error handling.
        """
        if end_year is None:
            end_year = start_year

        # Get all measurement files
        all_files = sorted(glob.glob(str(self.measurements_dir / "measurements_*.csv")))

        if not all_files:
            raise FileNotFoundError(f"No measurement files found in {self.measurements_dir}")

        # Filter by year and month
        files_to_load = []
        for file in all_files:
            filename = Path(file).name
            # Parse filename: measurements_YYYY_MM.csv
            parts = filename.replace('measurements_', '').replace('.csv', '').split('_')

            if len(parts) == 2:
                try:
                    year = int(parts[0])
                    month = int(parts[1])

                    if start_year <= year <= end_year:
                        if months is None or month in months:
                            files_to_load.append((year, month, file))
                except ValueError:
                    logger.warning(f"Could not parse filename: {filename}")
                    continue

        files_to_load.sort()
        logger.info(f"Loading {len(files_to_load)} files")

        dfs = []
        for year, month, file in files_to_load:
            try:
                df = self.load_measurements_file_safe(file, nrows=nrows_per_file)

                # Add year/month columns for reference
                df['year'] = year
                df['month'] = month

                dfs.append(df)
                logger.info(f"========| {Path(file).name}: {len(df)} rows |========")

            except Exception as e:
                logger.warning(f" ❌ Failed to load {Path(file).name}: {e}")
                continue

        if not dfs:
            return pd.DataFrame()

        # Combine all dataframes
        combined_df = pd.concat(dfs, ignore_index=True, sort=False)

        # Try to parse date column
        date_candidates = ['date', 'timestamp', 'datetime', 'time']
        for col in date_candidates:
            if col in combined_df.columns:
                try:
                    combined_df['date'] = pd.to_datetime(combined_df[col], errors='coerce')
                    if col != 'date':
                        combined_df = combined_df.drop(col, axis=1)
                    break
                except:
                    continue

        logger.info(f"Total rows loaded: {len(combined_df):,}")
        return combined_df

    def quick_sample(self, year: int = 2025, month: int = 1, nrows: int = 10000) -> pd.DataFrame:
        """
        Quick method to get a sample for testing.
        """
        return self.load_measurements_range(
            start_year=year,
            end_year=year,
            months=[month],
            nrows_per_file=nrows
        )

    def get_pm25_data(self,
                      years: Union[int, List[int], Tuple[int, int]] = 2025,
                      sample_size: Optional[int] = None) -> pd.DataFrame:
        """
        Get PM2.5 data with proper column detection.
        """
        # Parse years
        if isinstance(years, int):
            start_year = end_year = years
        elif isinstance(years, (list, tuple)) and len(years) == 2:
            start_year, end_year = years
        else:
            raise ValueError("years must be int or (start, end) tuple")

        # Load data
        df = self.load_measurements_range(start_year, end_year, nrows_per_file=sample_size)

        if len(df) == 0:
            return df

        # Try to find PM2.5 column
        pm25_candidates = ['pm25', 'PM2.5', 'pm2_5', 'pm25_1', 'pm25_2', 'pm25_value']
        pm25_col = None

        for col in pm25_candidates:
            if col in df.columns:
                pm25_col = col
                break

        if pm25_col is None:
            # Look for any column containing 'pm25'
            for col in df.columns:
                if 'pm25' in str(col).lower():
                    pm25_col = col
                    break

        if pm25_col:
            # Rename to standard
            if pm25_col != 'pm25':
                df = df.rename(columns={pm25_col: 'pm25'})

            # Filter out invalid values
            df = df[df['pm25'].notna()]
            df = df[df['pm25'] >= 0]
            df = df[df['pm25'] < 1000]  # Remove extreme outliers

            logger.info(f"PM2.5 data: {len(df):,} valid rows")
        else:
            logger.warning("No PM2.5 column found")

        return df