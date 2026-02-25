"""
Final data loader that properly handles the actual CSV format.
First line is "SET", second line contains headers, then data.
"""
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional, Union, List, Dict, Tuple
import logging
from datetime import datetime
import glob
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AirQualityDataLoader:
    """
    Data loader for airquality.am CSV files.
    Handles the specific format: line 1 = "SET", line 2 = headers, then data.
    """

    def __init__(self, data_dir: Union[str, Path]):
        """
        Initialize the data loader.

        Args:
            data_dir: Path to the directory containing the data files
        """
        # Convert to absolute path
        self.data_dir = Path(data_dir).absolute()
        self.measurements_dir = self.data_dir / "measurements"
        self.sensors_file = self.data_dir / "sensors.csv"
        self.sensors_df = None

        # Debug information
        print(f"\n[DataLoader Debug]")
        print(f"  Current working directory: {Path.cwd()}")
        print(f"  data_dir: {self.data_dir}")
        print(f"  data_dir exists: {self.data_dir.exists()}")
        print(f"  measurements_dir: {self.measurements_dir}")
        print(f"  measurements_dir exists: {self.measurements_dir.exists()}")
        print(f"  sensors_file: {self.sensors_file}")
        print(f"  sensors_file exists: {self.sensors_file.exists()}")

        if self.measurements_dir.exists():
            csv_files = list(self.measurements_dir.glob("*.csv"))
            print(f"  Found {len(csv_files)} measurement files")
        else:
            print(f"  WARNING: measurements directory not found!")
            # Try to create it
            try:
                self.measurements_dir.mkdir(parents=True, exist_ok=True)
                print(f"  Created measurements directory: {self.measurements_dir}")
            except Exception as e:
                print(f"  Could not create measurements directory: {e}")

        logger.info(f"Data loader initialized with directory: {self.data_dir}")

    def load_sensors_metadata(self) -> pd.DataFrame:
        """Load sensor metadata."""
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
            'title': 'location_name'
        }

        df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
        self.sensors_df = df
        logger.info(f"Loaded {len(df)} sensors")
        return df

    def load_measurement_file(self, file_path: Union[str, Path], nrows: Optional[int] = None) -> pd.DataFrame:
        """
        Load a single measurement file, skipping the first line.

        Args:
            file_path: Path to CSV file
            nrows: Number of rows to read (optional)
        """
        file_path = Path(file_path)
        logger.info(f"Loading {file_path.name}")

        # Skip first line (contains "SET"), use second line as header
        df = pd.read_csv(
            file_path,
            skiprows=1,  # Skip the "SET" line
            nrows=nrows
        )

        logger.info(f"  Loaded {len(df)} rows, {len(df.columns)} columns")
        return df

    def load_measurements_range(self,
                               start_year: int,
                               end_year: Optional[int] = None,
                               months: Optional[List[int]] = None,
                               sensors: Optional[List[int]] = None,
                               nrows_per_file: Optional[int] = None) -> pd.DataFrame:
        """
        Load measurements for a specific time range.

        Args:
            start_year: Starting year
            end_year: Ending year (defaults to start_year)
            months: List of months to load (None for all)
            sensors: List of sensor IDs to load (None for all)
            nrows_per_file: Number of rows per file (for sampling)
        """
        if end_year is None:
            end_year = start_year

        # Check if measurements directory exists
        if not self.measurements_dir.exists():
            raise FileNotFoundError(
                f"Measurements directory not found: {self.measurements_dir}\n"
                f"Please ensure your CSV files are in: {self.measurements_dir}"
            )

        # Get all measurement files
        all_files = sorted(glob.glob(str(self.measurements_dir / "measurements_*.csv")))

        if not all_files:
            raise FileNotFoundError(
                f"No measurement files found in {self.measurements_dir}\n"
                f"Looking for files matching: measurements_*.csv"
            )

        logger.info(f"Found {len(all_files)} total measurement files")

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
                            logger.debug(f"  Selected: {filename}")
                except ValueError:
                    logger.warning(f"Could not parse filename: {filename}")
                    continue

        files_to_load.sort()
        logger.info(f"Loading {len(files_to_load)} files for years {start_year}-{end_year}")

        if not files_to_load:
            logger.warning(f"No files found for years {start_year}-{end_year} with months {months}")
            return pd.DataFrame()

        dfs = []
        total_rows = 0

        for year, month, file in files_to_load:
            try:
                df = self.load_measurement_file(file, nrows=nrows_per_file)

                # Adding year/month columns
                df['year'] = year
                df['month'] = month

                # Parsing timestamp to datetime
                if 'timestamp' in df.columns:
                    df['datetime'] = pd.to_datetime(df['timestamp'])
                    df['date'] = df['datetime'].dt.date
                    df['hour'] = df['datetime'].dt.hour

                # Filter by sensors if specified
                if sensors and 'sensor_id' in df.columns:
                    df = df[df['sensor_id'].isin(sensors)]

                if len(df) > 0:
                    dfs.append(df)
                    total_rows += len(df)
                    logger.info(f"========| {Path(file).name}: {len(df)} rows |========")

            except Exception as e:
                logger.warning(f"========| Failed to load {Path(file).name}: {e} |========")
                continue

        if not dfs:
            logger.warning("No data loaded from any files")
            return pd.DataFrame()

        # Combine all dataframes
        combined_df = pd.concat(dfs, ignore_index=True, sort=False)
        logger.info(f"Total rows loaded: {total_rows:,}")

        return combined_df

    def get_pm25_data(self,
                     years: Union[int, List[int], Tuple[int, int]] = 2025,
                     months: Optional[List[int]] = None,
                     sensors: Optional[List[int]] = None,
                     sample_size: Optional[int] = None,
                     include_metadata: bool = True) -> pd.DataFrame:
        """
        Get PM2.5 data with proper column handling.

        Args:
            years: Single year, or (start_year, end_year) tuple
            months: List of months to include
            sensors: List of sensor IDs to include
            sample_size: Rows per file for sampling
            include_metadata: Whether to merge with sensor metadata
        """
        # Parse years
        if isinstance(years, int):
            start_year = end_year = years
        elif isinstance(years, (list, tuple)) and len(years) == 2:
            start_year, end_year = years
        else:
            raise ValueError("years must be int or (start, end) tuple")

        logger.info(f"Loading PM2.5 data for years {start_year}-{end_year}, months {months}, sensors {sensors}")

        # Load data
        df = self.load_measurements_range(
            start_year=start_year,
            end_year=end_year,
            months=months,
            sensors=sensors,
            nrows_per_file=sample_size
        )

        if len(df) == 0:
            logger.warning("No data loaded")
            return df

        pm25_col = None
        for col in ['pm2.5', 'pm2.5_corrected', 'pm25']:
            if col in df.columns:
                pm25_col = col
                break

        if pm25_col is None:
            logger.warning(f"No PM2.5 column found. Available: {df.columns.tolist()}")
            return df

        # Rename to standard 'pm25'
        if pm25_col != 'pm25':
            df = df.rename(columns={pm25_col: 'pm25'})

        # Clean data
        df = df[df['pm25'].notna()]   # Remove NaN
        df = df[df['pm25'] >= 0]      # Remove negative values
        df = df[df['pm25'] < 1000]    # Remove extreme outliers

        logger.info(f"Valid PM2.5 measurements: {len(df):,}")

        # Merge with sensor metadata if requested
        if include_metadata and 'sensor_id' in df.columns:
            if self.sensors_df is None:
                self.load_sensors_metadata()

            # Ensure same data type
            df['sensor_id'] = df['sensor_id'].astype(int)
            sensors_df = self.sensors_df.copy()
            sensors_df['sensor_id'] = sensors_df['sensor_id'].astype(int)

            df = df.merge(sensors_df, on='sensor_id', how='left')
            logger.info(f"Merged with sensor metadata: {len(df)} rows")

        return df

    def get_daily_averages(self,
                          years: Union[int, Tuple[int, int]] = 2025,
                          months: Optional[List[int]] = None) -> pd.DataFrame:
        """
        Calculate daily average PM2.5 per sensor.
        """
        df = self.get_pm25_data(years=years, months=months, include_metadata=True)

        if len(df) == 0:
            return df

        # Group by date and sensor
        daily_avg = df.groupby(['date', 'sensor_id', 'station_id', 'latitude', 'longitude']).agg({
            'pm25': ['mean', 'std', 'min', 'max', 'count']
        }).round(2)

        # Flatten column names
        daily_avg.columns = ['pm25_mean', 'pm25_std', 'pm25_min', 'pm25_max', 'measurements_count']
        daily_avg = daily_avg.reset_index()

        # Filter days with sufficient measurements (at least 18 hours of data)
        daily_avg = daily_avg[daily_avg['measurements_count'] >= 18]

        logger.info(f"Daily averages calculated: {len(daily_avg)} rows")
        return daily_avg

    def get_data_summary(self) -> Dict:
        """Get summary of available data."""
        summary = {
            'data_directory': str(self.data_dir),
            'measurements_directory': str(self.measurements_dir),
            'measurements_directory_exists': self.measurements_dir.exists()
        }

        # Sensors
        if self.sensors_file.exists():
            sensors_df = pd.read_csv(self.sensors_file)
            summary['sensors'] = {
                'total': len(sensors_df),
                'columns': sensors_df.columns.tolist(),
                'file_exists': True,
                'file_path': str(self.sensors_file)
            }
        else:
            summary['sensors'] = {
                'file_exists': False,
                'file_path': str(self.sensors_file)
            }

        # Measurement files
        if self.measurements_dir.exists():
            files = glob.glob(str(self.measurements_dir / "measurements_*.csv"))
            files.sort()

            file_info = []
            years = set()
            total_size = 0

            for file in files:
                filename = Path(file).name
                size_mb = Path(file).stat().st_size / (1024 * 1024)
                total_size += size_mb

                parts = filename.replace('measurements_', '').replace('.csv', '').split('_')
                if len(parts) == 2:
                    try:
                        years.add(int(parts[0]))
                    except:
                        pass

                file_info.append({
                    'filename': filename,
                    'size_mb': round(size_mb, 2)
                })

            summary['measurements'] = {
                'directory_exists': True,
                'file_count': len(file_info),
                'total_size_gb': round(total_size / 1024, 2),
                'years': sorted(years),
                'latest_files': file_info[-5:] if file_info else []
            }
        else:
            summary['measurements'] = {
                'directory_exists': False,
                'file_count': 0,
                'years': []
            }

        return summary