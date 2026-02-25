import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import seaborn as sns
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent.parent))

from src.data.data_loader_final import AirQualityDataLoader

class AirQualityVisualizer:
    """
    Professional visualizations for air quality data.
    """

    def __init__(self, data_loader=None):
        self.loader = data_loader or AirQualityDataLoader(Path("data/raw"))
        plt.style.use('seaborn-v0_8-darkgrid')
        sns.set_palette("husl")

    def plot_sensor_timeseries(self, sensor_id=41, year=2025, month=1, save=True):
        """
        Create time series plot for a specific sensor.
        """
        # Load data
        df = self.loader.get_pm25_data(years=year, months=[month], sensors=[sensor_id])

        if len(df) == 0:
            print(f"No data for sensor {sensor_id}")
            return

        # Create figure
        fig, axes = plt.subplots(2, 1, figsize=(15, 10))

        # Daily pattern
        ax = axes[0]
        ax.plot(df['datetime'], df['pm25'], 'b-', linewidth=0.5, alpha=0.7)
        ax.axhline(y=5, color='green', linestyle='--', alpha=0.5, label='WHO Guideline (5)')
        ax.axhline(y=15, color='orange', linestyle='--', alpha=0.5, label='WHO Target (15)')
        ax.axhline(y=25, color='red', linestyle='--', alpha=0.5, label='WHO Interim (25)')
        ax.set_xlabel('Date')
        ax.set_ylabel('PM2.5 (µg/m³)')
        ax.set_title(f'Sensor {sensor_id} - PM2.5 Time Series')
        ax.legend()
        ax.grid(True, alpha=0.3)

        # Hourly pattern
        ax = axes[1]
        hourly_avg = df.groupby(df['datetime'].dt.hour)['pm25'].agg(['mean', 'std'])
        ax.plot(hourly_avg.index, hourly_avg['mean'], 'ro-', linewidth=2, markersize=8)
        ax.fill_between(hourly_avg.index,
                        hourly_avg['mean'] - hourly_avg['std'],
                        hourly_avg['mean'] + hourly_avg['std'],
                        alpha=0.2, color='red')
        ax.set_xlabel('Hour of Day')
        ax.set_ylabel('PM2.5 (µg/m³)')
        ax.set_title(f'Sensor {sensor_id} - Average Daily Pattern')
        ax.grid(True, alpha=0.3)

        plt.tight_layout()

        if save:
            filename = f'sensor_{sensor_id}_analysis.png'
            plt.savefig(filename, dpi=150, bbox_inches='tight')
            print(f"========| Plot saved as '{filename}' |========")

        plt.show()

    def plot_district_comparison(self, year=2025, month=1, top_n=10, save=True):
        """
        Compare PM2.5 levels across districts.
        """
        # Load data for multiple sensors
        df = self.loader.get_pm25_data(years=year, months=[month], include_metadata=True)

        if len(df) == 0:
            print("No data loaded")
            return

        # Aggregate by district
        district_stats = df.groupby('district_slug').agg({
            'pm25': ['mean', 'std', 'count']
        }).round(2)
        district_stats.columns = ['mean', 'std', 'count']
        district_stats = district_stats.sort_values('mean', ascending=False).head(top_n)

        # Create figure
        fig, axes = plt.subplots(1, 2, figsize=(15, 6))

        # Bar plot
        ax = axes[0]
        districts = district_stats.index
        means = district_stats['mean']
        errors = district_stats['std']

        ax.barh(range(len(districts)), means, xerr=errors, capsize=5)
        ax.set_yticks(range(len(districts)))
        ax.set_yticklabels(districts)
        ax.set_xlabel('Mean PM2.5 (µg/m³)')
        ax.set_title(f'Top {top_n} Districts by PM2.5')
        ax.axvline(x=5, color='green', linestyle='--', alpha=0.7, label='WHO Guideline')
        ax.legend()

        # Box plot
        ax = axes[1]
        data_to_plot = [df[df['district_slug'] == d]['pm25'].values for d in districts]
        bp = ax.boxplot(data_to_plot, labels=districts, vert=False)
        ax.set_xlabel('PM2.5 (µg/m³)')
        ax.set_title('PM2.5 Distribution by District')
        ax.axvline(x=5, color='green', linestyle='--', alpha=0.7, label='WHO Guideline')
        ax.legend()

        plt.tight_layout()

        if save:
            filename = f'district_comparison_{year}_{month:02d}.png'
            plt.savefig(filename, dpi=150, bbox_inches='tight')
            print(f"========| Plot saved as '{filename}' |========")

        plt.show()

    def create_dashboard_plots(self, year=2025, month=1):
        """
        Create a set of plots for a dashboard.
        """
        print("\nCreating dashboard visualizations...")

        # 1. City-wide time series
        df = self.loader.get_pm25_data(years=year, months=[month])
        city_daily = df.groupby(df['datetime'].dt.date)['pm25'].mean()

        fig, ax = plt.subplots(figsize=(12, 5))
        ax.plot(city_daily.index, city_daily.values, 'b-', linewidth=2)
        ax.fill_between(city_daily.index, city_daily.values, alpha=0.3)
        ax.set_xlabel('Date')
        ax.set_ylabel('PM2.5 (µg/m³)')
        ax.set_title(f'Yerevan - Daily Average PM2.5 ({year}-{month:02d})')
        ax.axhline(y=5, color='green', linestyle='--', label='WHO Guideline')
        ax.axhline(y=15, color='orange', linestyle='--', label='WHO Target')
        ax.legend()
        ax.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig('city_daily_average.png', dpi=150)

        # 2. Distribution
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.hist(df['pm25'], bins=50, edgecolor='black', alpha=0.7)
        ax.axvline(df['pm25'].mean(), color='red', linestyle='--',
                  label=f'Mean: {df["pm25"].mean():.1f}')
        ax.axvline(df['pm25'].median(), color='green', linestyle='--',
                  label=f'Median: {df["pm25"].median():.1f}')
        ax.set_xlabel('PM2.5 (µg/m³)')
        ax.set_ylabel('Frequency')
        ax.set_title('Distribution of PM2.5 Measurements')
        ax.legend()
        ax.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig('pm25_distribution.png', dpi=150)

        print("========| Dashboard plots created: city_daily_average.png, pm25_distribution.png |========")

def main():
    """Create visualizations."""
    viz = AirQualityVisualizer()

    # Plot for a specific sensor
    viz.plot_sensor_timeseries(sensor_id=41, year=2025, month=1)

    # Compare districts
    viz.plot_district_comparison(year=2025, month=1)

    # Create dashboard
    viz.create_dashboard_plots(year=2025, month=1)

if __name__ == "__main__":
    main()