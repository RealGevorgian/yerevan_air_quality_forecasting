"""
Test the final data loader with the correct CSV format.
"""
import sys
from pathlib import Path
import pandas as pd

# Add src to path
sys.path.append(str(Path(__file__).parent))

from src.data.data_loader_final import AirQualityDataLoader


def test_loader():
    """Test the final loader."""

    print("=" * 60)
    print("TESTING FINAL DATA LOADER")
    print("=" * 60)

    # Initialize
    data_dir = Path("data/raw")
    loader = AirQualityDataLoader(data_dir)

    # Test 1: Load sensors
    print("\n1. LOADING SENSORS")
    print("-" * 40)
    sensors = loader.load_sensors_metadata()
    print(f"Total sensors: {len(sensors)}")
    print(f"Columns: {list(sensors.columns)}")
    print("\nFirst 3 sensors:")
    print(sensors.head(3).to_string())

    # Test 2: Load small sample of measurements
    print("\n2. LOADING SAMPLE MEASUREMENTS")
    print("-" * 40)
    print("Loading January 2025 (1000 rows per file)...")

    df_sample = loader.get_pm25_data(
        years=2025,
        months=[1],
        sample_size=1000,
        include_metadata=True
    )

    if len(df_sample) > 0:
        print(f"\n✅ Loaded {len(df_sample):,} rows")
        print(f"Columns: {list(df_sample.columns)}")
        print(f"Date range: {df_sample['datetime'].min()} to {df_sample['datetime'].max()}")
        print(f"Number of sensors: {df_sample['sensor_id'].nunique()}")

        print("\nPM2.5 Statistics:")
        print(df_sample['pm25'].describe())

        print("\nFirst 5 rows:")
        print(df_sample[['datetime', 'sensor_id', 'pm25', 'station_id', 'latitude']].head(5).to_string())

    # Test 3: Calculate daily averages
    print("\n3. CALCULATING DAILY AVERAGES")
    print("-" * 40)

    daily_df = loader.get_daily_averages(years=2025, months=[1])

    if len(daily_df) > 0:
        print(f"Daily averages: {len(daily_df)} rows")
        print("\nFirst 5 daily averages:")
        print(daily_df.head(5).to_string())

    # Test 4: Data summary
    print("\n4. DATA SUMMARY")
    print("-" * 40)
    summary = loader.get_data_summary()

    print(f"Measurement files: {summary['measurements']['file_count']}")
    print(f"Total size: {summary['measurements']['total_size_gb']} GB")
    print(f"Years available: {summary['measurements']['years']}")

    print("\n" + "=" * 60)
    print("TEST COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    test_loader()