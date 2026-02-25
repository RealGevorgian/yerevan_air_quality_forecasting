"""
Test the fixed data loader.
"""
import sys
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent))

from src.data.data_loader_fixed import AirQualityDataLoaderFixed


def test_loader():
    """Test the fixed loader with a small sample."""

    print("=" * 60)
    print("TESTING FIXED DATA LOADER")
    print("=" * 60)

    # Initialize
    data_dir = Path("data/raw")
    loader = AirQualityDataLoaderFixed(data_dir)

    # Test 1: Load sensors
    print("\n1. Testing sensor loading...")
    sensors = loader.load_sensors_metadata()
    print(f"   Sensors loaded: {len(sensors)}")

    # Test 2: Load a tiny sample (just 1000 rows per file)
    print("\n2. Testing measurement loading (sample)...")
    df_sample = loader.quick_sample(year=2025, month=1, nrows=1000)

    if len(df_sample) > 0:
        print(f"   ✅ Sample loaded: {len(df_sample)} rows")
        print(f"   Columns: {list(df_sample.columns)}")

        # Show first few rows
        print("\n   First 3 rows:")
        print(df_sample.head(3).to_string())

        # Test 3: Get PM2.5 data
        print("\n3. Extracting PM2.5 data...")
        df_pm25 = loader.get_pm25_data(years=2025, sample_size=1000)

        if len(df_pm25) > 0 and 'pm25' in df_pm25.columns:
            print(f"   PM2.5 rows: {len(df_pm25)}")
            print(f"   PM2.5 stats:")
            print(f"     Mean: {df_pm25['pm25'].mean():.2f}")
            print(f"     Min: {df_pm25['pm25'].min():.2f}")
            print(f"     Max: {df_pm25['pm25'].max():.2f}")
    else:
        print("   ❌ Failed to load sample")

    print("\n" + "=" * 60)
    print("TEST COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    test_loader()