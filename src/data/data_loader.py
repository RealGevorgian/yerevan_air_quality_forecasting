"""
Enhanced data loading module for airquality.am dataset.
Handles raw measurement files and merges with sensor metadata.
"""
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional, Union, List, Dict, Tuple
import logging
from datetime import datetime, timedelta
import glob
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class AirQualityDataLoader:
    """Load air quality data from airquality.am CSV files."""

    def __init__(self, data_dir: Union[str, Path]):
        """
        Initialize the data loader.

        Args:
            data_dir: Path to the directory containing the data files
        """
        self.data_dir = Path(data_dir)
        self.measurements_dir = self.data_dir / "measurements"
        self.sensors_file = self.data_dir / "sensors.csv"
        self.sensors_df = None
        logger.info(f"Data loader initialized with directory: {self.data_dir}")

    def load_sensors_metadata(self, force_reload: bool = False) -> pd.DataFrame:
        """
        Load sensor location and metadata from sensors.csv.

        Args:
            force_reload: Force reload even if already loaded
        """
        if self.sensors_df is not None and not force_reload:
            return self.sensors_df

        logger.info(f"Loading sensors metadata from {self.sensors_file}")

        if not self.sensors_file.exists():
            raise FileNotFoundError(f"Sensors file not found: {self.sensors_file}")

        df = pd.read_csv(self.sensors_file)

        # Basic cleaning
        df = df.drop_duplicates(subset=['sensor_id'] if 'sensor_id' in df.columns else None)

        # Ensure consistent column names
        column_mapping = {
            'sensor_id': 'sensor_id',
            'station_id': 'station_id',
            'latitude': 'latitude',
            'longitude': 'longitude',
            'height': 'height',
            'type': 'sensor_type',
            'param': 'parameter'
        }

        df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})

        self.sensors_df = df
        logger.info(f"========| Loaded {len(df)} sensors |========")
        logger.info(f"   Columns: {df.columns.tolist()}")

        return df

    def list_available_measurements(self) -> pd.DataFrame:
        """
        List all available measurement files with their properties.

        Returns:
            DataFrame with file information
        """
        if not self.measurements_dir.exists():
            raise FileNotFoundError(f"Measurements directory not found: {self.measurements_dir}")

        files = glob.glob(str(self.measurements_dir / "measurements_*.csv"))
        files.sort()

        file_info = []
        for file in files:
            filename = Path(file).name
            # Parse year and month from filename
            parts = filename.replace('measurements_', '').replace('.csv', '').split('_')

            if len(parts) == 2:
                year, month = parts
                file_size = os.path.getsize(file) / (1024 * 1024)  # Size in MB

                file_info.append({
                    'filename': filename,
                    'year': int(year),
                    'month': int(month),
                    'path': file,
                    'size_mb': round(file_size, 2),
                    'exists': True
                })

        df = pd.DataFrame(file_info)
        logger.info(f"Found {len(df)} measurement files")
        return df

    def load_measurements_file(self,
                               file_path: Union[str, Path],
                               sample: Optional[float] = None) -> pd.DataFrame:
        """
        Load a single measurements CSV file.

        Args:
            file_path: Path to the measurement file
            sample: If provided, load only a sample fraction (0.0-1.0)
        """
        file_path = Path(file_path)
        logger.info(f"Loading measurements from {file_path.name}")

        # Load CSV with optimizations
        if sample:
            # Load a random sample
            df = pd.read_csv(file_path, skiprows=lambda i: i > 0 and np.random.random() > sample)
        else:
            df = pd.read_csv(file_path)

        # Parse date column
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
        elif 'timestamp' in df.columns:
            df['date'] = pd.to_datetime(df['timestamp'])
            df = df.drop('timestamp', axis=1)

        logger.info(f"   Loaded {len(df):,} rows")
        return df

    def load_measurements_range(self,
                                start_year: int,
                                end_year: Optional[int] = None,
                                months: Optional[List[int]] = None,
                                sensors: Optional[List[int]] = None,
                                sample: Optional[float] = None) -> pd.DataFrame:
        """
        Load measurements for a specific time range.

        Args:
            start_year: Starting year
            end_year: Ending year (inclusive, defaults to start_year)
            months: List of months to load (None for all months)
            sensors: List of sensor IDs to load (None for all sensors)
            sample: Sample fraction for large files
        """
        if end_year is None:
            end_year = start_year

        available_files = self.list_available_measurements()

        # Filter files by year range
        mask = (available_files['year'] >= start_year) & (available_files['year'] <= end_year)

        if months:
            mask = mask & (available_files['month'].isin(months))

        files_to_load = available_files[mask]

        if len(files_to_load) == 0:
            raise ValueError(f"No measurement files found for years {start_year}-{end_year}")

        logger.info(f"Loading {len(files_to_load)} files from {start_year} to {end_year}")

        dfs = []
        total_rows = 0

        for _, file_info in files_to_load.iterrows():
            df = self.load_measurements_file(file_info['path'], sample=sample)

            # Filter by sensors if specified
            if sensors and 'sensor_id' in df.columns:
                df = df[df['sensor_id'].isin(sensors)]

            dfs.append(df)
            total_rows += len(df)

        if not dfs:
            return pd.DataFrame()

        # Combine all dataframes
        combined_df = pd.concat(dfs, ignore_index=True)
        combined_df = combined_df.sort_values('date').reset_index(drop=True)

        logger.info(f"✅ Loaded {len(combined_df):,} total rows")
        logger.info(f"   Date range: {combined_df['date'].min()} to {combined_df['date'].max()}")

        return combined_df

    def load_recent_measurements(self, days: int = 30) -> pd.DataFrame:
        """
        Load measurements from the last N days.

        Args:
            days: Number of recent days to load
        """
        available_files = self.list_available_measurements()

        if len(available_files) == 0:
            raise ValueError("No measurement files found")

        # Get the most recent files
        latest_files = available_files.sort_values(['year', 'month'], ascending=False).head(3)

        logger.info(f"Loading recent data from {len(latest_files)} files")

        dfs = []
        cutoff_date = datetime.now() - timedelta(days=days)

        for _, file_info in latest_files.iterrows():
            df = self.load_measurements_file(file_info['path'])

            # Filter to recent dates
            if 'date' in df.columns:
                df = df[df['date'] >= cutoff_date]

            if len(df) > 0:
                dfs.append(df)

        if not dfs:
            logger.warning(f"No data found for the last {days} days")
            return pd.DataFrame()

        combined_df = pd.concat(dfs, ignore_index=True)
        combined_df = combined_df.sort_values('date').reset_index(drop=True)

        logger.info(f"✅ Loaded {len(combined_df):,} rows from the last {days} days")
        return combined_df

    def merge_with_sensor_metadata(self,
                                   measurements_df: pd.DataFrame,
                                   how: str = 'left') -> pd.DataFrame:
        """
        Merge measurements with sensor metadata.

        Args:
            measurements_df: DataFrame with measurement data
            how: Type of merge to perform
        """
        if self.sensors_df is None:
            self.load_sensors_metadata()

        if 'sensor_id' not in measurements_df.columns:
            logger.warning("No sensor_id column in measurements data")
            return measurements_df

        # Ensure sensor_id is the same type in both dataframes
        measurements_df['sensor_id'] = measurements_df['sensor_id'].astype(str)
        sensors_df = self.sensors_df.copy()
        sensors_df['sensor_id'] = sensors_df['sensor_id'].astype(str)

        # Merge
        merged_df = measurements_df.merge(sensors_df, on='sensor_id', how=how)

        logger.info(f"Merged data: {len(merged_df):,} rows")
        logger.info(f"   Columns after merge: {merged_df.columns.tolist()}")

        return merged_df

    def get_pm25_data(self,
                      years: Union[int, List[int], Tuple[int, int]] = 2025,
                      include_metadata: bool = True,
                      clean_negative: bool = True) -> pd.DataFrame:
        """
        Convenience method to get PM2.5 data.

        Args:
            years: Single year, list of years, or (start_year, end_year) tuple
            include_metadata: Whether to merge with sensor metadata
            clean_negative: Whether to filter out negative values
        """
        # Parse years parameter
        if isinstance(years, int):
            start_year = end_year = years
        elif isinstance(years, (list, tuple)) and len(years) == 2:
            start_year, end_year = years
        else:
            raise ValueError("years must be int, list of ints, or (start, end) tuple")

        # Load measurements
        df = self.load_measurements_range(start_year, end_year)

        if len(df) == 0:
            logger.warning("No data loaded")
            return df

        # Find PM2.5 column
        pm25_cols = ['pm25', 'PM2.5', 'pm2_5']
        actual_pm25_col = None

        for col in pm25_cols:
            if col in df.columns:
                actual_pm25_col = col
                break

        if actual_pm25_col is None:
            logger.warning(f"No PM2.5 column found. Available: {df.columns.tolist()}")
            return df

        # Filter to only PM2.5 data if there are multiple parameters
        if 'parameter' in df.columns:
            df = df[df['parameter'].str.lower().str.contains('pm25|pm2.5', na=False)]

        # Clean negative values if requested
        if clean_negative and actual_pm25_col in df.columns:
            neg_count = (df[actual_pm25_col] < 0).sum()
            if neg_count > 0:
                df = df[df[actual_pm25_col] >= 0]
                logger.info(f"Removed {neg_count} rows with negative PM2.5 values")

        # Rename PM2.5 column to standard name
        if actual_pm25_col != 'pm25':
            df = df.rename(columns={actual_pm25_col: 'pm25'})

        # Merge with metadata if requested
        if include_metadata:
            df = self.merge_with_sensor_metadata(df)

        logger.info(f"✅ Final PM2.5 dataset: {len(df):,} rows")
        return df

    def get_data_summary(self) -> Dict:
        """Get a comprehensive summary of available data."""
        summary = {
            'data_directory': str(self.data_dir),
            'measurements_directory': str(self.measurements_dir) if self.measurements_dir.exists() else 'Not found',
            'sensors_file': str(self.sensors_file) if self.sensors_file.exists() else 'Not found'
        }

        # Check sensors file
        if self.sensors_file.exists():
            sensors_df = pd.read_csv(self.sensors_file)
            summary['sensors'] = {
                'exists': True,
                'count': len(sensors_df),
                'columns': sensors_df.columns.tolist(),
                'size_mb': round(self.sensors_file.stat().st_size / (1024 * 1024), 2)
            }
        else:
            summary['sensors'] = {'exists': False}

        # Check measurement files
        if self.measurements_dir.exists():
            files = glob.glob(str(self.measurements_dir / "measurements_*.csv"))
            files.sort()

            file_list = []
            total_size = 0
            years = set()

            for file in files:
                filename = Path(file).name
                size_mb = os.path.getsize(file) / (1024 * 1024)
                total_size += size_mb

                # Extract year
                parts = filename.replace('measurements_', '').replace('.csv', '').split('_')
                if len(parts) == 2:
                    years.add(int(parts[0]))

                file_list.append({
                    'filename': filename,
                    'size_mb': round(size_mb, 2)
                })

            summary['measurements'] = {
                'exists': True,
                'file_count': len(file_list),
                'total_size_gb': round(total_size / 1024, 2),
                'years_available': sorted(list(years)),
                'files': file_list[-10:]  # Show last 10 files
            }
        else:
            summary['measurements'] = {'exists': False}

        return summary