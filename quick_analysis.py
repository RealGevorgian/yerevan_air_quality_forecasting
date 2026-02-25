"""
Quick analysis script to see immediate results from your air quality data.
"""
import sys
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

# Add src to path
sys.path.append(str(Path(__file__).parent))

from src.data.data_loader import AirQualityDataLoader


def quick_analysis():
    """Run a quick analysis and show results."""

    print("=" * 60)
    print("AIR QUALITY DATA ANALYSIS - QUICK RESULTS")
    print("=" * 60)

    # Initialize loader
    data_dir = Path("data/raw")
    loader = AirQualityDataLoader(data_dir)

    # 1. Load sensor metadata
    print("\n1. LOADING SENSOR METADATA")
    print("-" * 40)
    sensors = loader.load_sensors_metadata()
    print(f"Total sensors: {len(sensors)}")
    print(f"Columns: {list(sensors.columns)}")
    print("\nFirst 3 sensors:")
    print(sensors.head(3).to_string())

    # 2. Load a small sample of measurements (just one month)
    print("\n2. LOADING SAMPLE MEASUREMENTS")
    print("-" * 40)
    print("Loading January 2025 data...")

    df_sample = loader.load_measurements_range(
        start_year=2025,
        end_year=2025,
        months=[1]  # Just January
    )

    print(f"Rows loaded: {len(df_sample):,}")
    print(f"Columns: {list(df_sample.columns)}")

    # 3. Basic PM2.5 statistics
    print("\n3. PM2.5 STATISTICS (January 2025)")
    print("-" * 40)

    # Find PM2.5 column
    pm25_col = None
    for col in ['pm25', 'PM2.5', 'pm2_5']:
        if col in df_sample.columns:
            pm25_col = col
            break

    if pm25_col:
        # Filter out negative values
        pm25_values = df_sample[pm25_col][df_sample[pm25_col] >= 0]

        print(f"Using column: {pm25_col}")
        print(f"Total measurements: {len(pm25_values):,}")
        print(f"Mean: {pm25_values.mean():.2f} µg/m³")
        print(f"Median: {pm25_values.median():.2f} µg/m³")
        print(f"Std Dev: {pm25_values.std():.2f}")
        print(f"Min: {pm25_values.min():.2f}")
        print(f"Max: {pm25_values.max():.2f}")
        print(f"\nPercentiles:")
        for p in [10, 25, 50, 75, 90, 95, 99]:
            print(f"  {p}th: {pm25_values.quantile(p / 100):.2f}")

    # 4. Count by sensor
    print("\n4. MEASUREMENTS PER SENSOR")
    print("-" * 40)
    if 'sensor_id' in df_sample.columns:
        sensor_counts = df_sample['sensor_id'].value_counts().head(10)
        print("Top 10 sensors by number of measurements:")
        for sensor_id, count in sensor_counts.items():
            print(f"  Sensor {sensor_id}: {count:,} measurements")

    # 5. Simple plot
    print("\n5. GENERATING SIMPLE PLOT")
    print("-" * 40)

    if 'date' in df_sample.columns and pm25_col:
        # Create figure
        fig, axes = plt.subplots(2, 1, figsize=(12, 8))

        # Plot 1: Time series (first 1000 points for clarity)
        df_plot = df_sample.head(1000).sort_values('date')
        axes[0].plot(df_plot['date'], df_plot[pm25_col], 'b-', linewidth=0.5, alpha=0.7)
        axes[0].set_title('PM2.5 Time Series (First 1000 measurements)')
        axes[0].set_xlabel('Date')
        axes[0].set_ylabel('PM2.5 (µg/m³)')
        axes[0].grid(True, alpha=0.3)

        # Plot 2: Histogram
        axes[1].hist(pm25_values, bins=50, edgecolor='black', alpha=0.7, color='steelblue')
        axes[1].set_title('PM2.5 Distribution')
        axes[1].set_xlabel('PM2.5 (µg/m³)')
        axes[1].set_ylabel('Frequency')
        axes[1].axvline(pm25_values.mean(), color='red', linestyle='--',
                        label=f'Mean: {pm25_values.mean():.1f}')
        axes[1].axvline(pm25_values.median(), color='green', linestyle='--',
                        label=f'Median: {pm25_values.median():.1f}')
        axes[1].legend()

        plt.tight_layout()
        plt.savefig('quick_analysis_plot.png', dpi=100)
        print("Plot saved as 'quick_analysis_plot.png'")
        plt.show()

    # 6. Data quality check
    print("\n6. DATA QUALITY CHECK")
    print("-" * 40)

    total_rows = len(df_sample)
    missing_pm25 = df_sample[pm25_col].isna().sum() if pm25_col else 0
    negative_pm25 = (df_sample[pm25_col] < 0).sum() if pm25_col else 0

    print(f"Total rows: {total_rows:,}")
    if pm25_col:
        print(f"Missing PM2.5 values: {missing_pm25:,} ({missing_pm25 / total_rows * 100:.1f}%)")
        print(f"Negative PM2.5 values: {negative_pm25:,} ({negative_pm25 / total_rows * 100:.1f}%)")

    print("\n" + "=" * 60)
    print("ANALYSIS COMPLETE")
    print("=" * 60)

    return df_sample, sensors


if __name__ == "__main__":
    df, sensors = quick_analysis()