import sys
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import logging
import time

# Disable all logging for clean output
logging.getLogger().setLevel(logging.ERROR)

# Add project root to Python path
project_root = Path(__file__).parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Import project modules
from src.data.data_loader_final import AirQualityDataLoader
from src.data.web_scraper import AirQualityScraper

class AirQualityMenu:
    """
    Clean, user-friendly CLI for air quality monitoring in Yerevan.
    """

    def __init__(self):
        self.data_dir = Path("data/raw")

        # Initialize data sources
        self.file_loader = AirQualityDataLoader(self.data_dir)
        self.web_scraper = AirQualityScraper()

        # Available years in dataset
        self.available_years = [2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026]

        # Sensor to district mapping
        self.sensor_districts = {
            2: "Nor Nork",
            4: "Arabkir",
            7: "Davtashen",
            9: "Erebuni",
            11: "Shengavit",
            28: "Ajapnyak",
            29: "Arabkir",
            30: "Davtashen",
            41: "Avan",
            43: "Shengavit",
            45: "Nor Nork",
            50: "Ajapnyak",
            53: "Kentron"
        }

        # Cache for live data
        self.live_cache = {}
        self.summary_stats = {}
        self.cache_time = None

        # Health risk thresholds (WHO guidelines)
        self.risk_levels = {
            (0, 5): ("Good", "No health impacts expected"),
            (5, 15): ("Moderate", "Unusually sensitive people should consider reducing prolonged outdoor exertion"),
            (15, 25): ("Unhealthy for Sensitive Groups", "Active children and adults with respiratory disease should limit outdoor exertion"),
            (25, 35): ("Unhealthy", "Everyone may begin to experience health effects"),
            (35, 50): ("Very Unhealthy", "Health warnings of emergency conditions"),
            (50, float('inf')): ("Hazardous", "Health alert: everyone may experience serious health effects")
        }

        # User profiles for advice
        self.profiles = {
            "1": "sensitive",
            "2": "athlete",
            "3": "elderly",
            "4": "parent"
        }

    def clear_screen(self):
        """Clear the terminal screen for cleaner interface."""
        print("\033c", end="")

    def print_header(self, title):
        """Print a formatted header."""
        print("\n" + "═" * 70)
        print(f" {title}")
        print("═" * 70)

    def print_menu(self):
        """Display the main menu."""
        print("\n" + "╔" + "═" * 66 + "╗")
        print("║                        YEREVAN AIR QUALITY                       ║")
        print("╚" + "═" * 66 + "╝")
        print("\n MAIN MENU:")
        print("  1. Check current air quality (LIVE)")
        print("  2. Get hourly forecast")
        print("  3. Compare air quality across locations")
        print("  4. Generate health risk report")
        print("  5. Analyze historical trends")
        print("  6. Get personalized health advice")
        print("  7. Draw air pollution diagram")
        print("  8. List all available sensors")
        print("  0. ❌ Exit ❌")
        print("\n" + "─" * 70)

    def display_sensor_options(self):
        """Display available sensors with their districts."""
        print("\nAvailable sensors:")
        by_district = {}
        for sensor_id, district in self.sensor_districts.items():
            by_district.setdefault(district, []).append(str(sensor_id))

        for district, sensors in sorted(by_district.items()):
            print(f"  {district:12}: {', '.join(sensors)}")

    def get_sensor_input(self, prompt="Enter sensor ID"):
        """Get valid sensor ID from user."""
        self.display_sensor_options()

        while True:
            try:
                sensor_id = int(input(f"\n{prompt}: ").strip())
                if sensor_id in self.sensor_districts:
                    return sensor_id
                print("  Invalid sensor ID. Please choose from the list above.")
            except ValueError:
                print("  Please enter a number.")
            except KeyboardInterrupt:
                print("\n")
                return None

    def get_year_input(self):
        """Get valid year from user."""
        while True:
            try:
                print(f"\nAvailable years: {', '.join(map(str, self.available_years))}")
                year = int(input("Enter year: ").strip())
                if year in self.available_years:
                    return year
                print(f"  Year must be between {self.available_years[0]} and {self.available_years[-1]}")
            except ValueError:
                print("  Please enter a valid year.")
            except KeyboardInterrupt:
                print("\n")
                return None

    def get_month_input(self):
        """Get valid month from user."""
        while True:
            try:
                month = int(input("Enter month (1-12): ").strip())
                if 1 <= month <= 12:
                    return month
                print("  Month must be between 1 and 12")
            except ValueError:
                print("  Please enter a number.")
            except KeyboardInterrupt:
                print("\n")
                return None

    def get_live_data(self, sensor_id):
        """
        Get live data from web scraping (cached for 5 minutes).
        """
        # Check cache
        if self.cache_time and time.time() - self.cache_time < 300:
            return self.live_cache.get(sensor_id)

        # Fetch new data
        print("\n  📡 Fetching live data from airquality.am...")
        readings = self.web_scraper.get_current_readings()

        # Update cache
        self.live_cache = {}
        for r in readings:
            if r.get('sensor_id'):
                self.live_cache[r['sensor_id']] = r

        # Store city-wide reading
        city_reading = self.web_scraper.get_city_reading()
        if city_reading:
            self.live_cache[0] = city_reading

        self.summary_stats = self.web_scraper.get_summary_stats()
        self.cache_time = time.time()

        return self.live_cache.get(sensor_id)

    def get_file_data(self, sensor_id, hours=24):
        """Get data from CSV files as fallback."""
        try:
            df = self.file_loader.get_pm25_data(
                years=2025,
                months=[1, 2],
                sensors=[sensor_id],
                sample_size=hours * 4
            )
            if df is not None and len(df) > 0:
                df = df.sort_values('datetime', ascending=False)
                return df.head(hours)
        except:
            pass
        return pd.DataFrame()

    def get_risk_level(self, pm25):
        """Get risk level based on PM2.5 value."""
        for (low, high), (level, _) in self.risk_levels.items():
            if low < pm25 <= high:
                return level
        return "Unknown"

    def calculate_excess_risk(self, pm25):
        """Calculate excess health risk percentage."""
        if pm25 <= 5:
            return {}
        excess = pm25 - 5
        return {
            'mortality': round((1.062 ** (excess/10) - 1) * 100, 1),
            'cardiovascular': round((1.11 ** (excess/10) - 1) * 100, 1),
            'respiratory': round((1.08 ** (excess/10) - 1) * 100, 1)
        }

    def option_1_current_quality(self):
        """Check current air quality with LIVE data."""
        self.print_header("🌍 CURRENT AIR QUALITY (LIVE)")

        sensor_id = self.get_sensor_input()
        if sensor_id is None:
            return

        district = self.sensor_districts.get(sensor_id, "Unknown")
        print(f"\nFetching data for sensor {sensor_id} ({district})...")

        # Try live data
        live_data = self.get_live_data(sensor_id)

        if live_data:
            pm25 = live_data['pm25']
            location = live_data.get('location', district)
            risk = self.get_risk_level(pm25)

            print("\n" + "┌" + "─" * 58 + "┐")
            print("│                            RESULTS                            │")
            print("├" + "─" * 58 + "┤")
            print(f"│ Sensor:     {sensor_id} ({district})")
            print(f"│ Location:   {location}")
            print(f"│ Time:       {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} (LIVE)")
            print(f"│ PM2.5:      {pm25:.1f} µg/m³")
            print(f"│ Risk Level: {risk}")
            print("└" + "─" * 58 + "┘")
        else:
            print("\n  Live data unavailable, using file data...")
            df = self.get_file_data(sensor_id, hours=24)

            if len(df) == 0:
                print(f"\n  No data available for sensor {sensor_id}")
                return

            latest = df.iloc[0]
            pm25 = latest['pm25']
            risk = self.get_risk_level(pm25)

            print("\n" + "┌" + "─" * 58 + "┐")
            print("│                            RESULTS                            │")
            print("├" + "─" * 58 + "┤")
            print(f"│ Sensor:     {sensor_id} ({district})")
            print(f"│ Time:       {latest['datetime']}")
            print(f"│ PM2.5:      {pm25:.1f} µg/m³")
            print(f"│ Risk Level: {risk}")
            print("└" + "─" * 58 + "┘")

        # Health impact
        if pm25 > 5:
            risks = self.calculate_excess_risk(pm25)
            print("\nHealth Impact:")
            for outcome, value in risks.items():
                print(f"  • {outcome.title()}: {value}% excess risk")

        # Show city summary if available
        if self.summary_stats:
            print("\nYerevan Summary:")
            if 'daily_avg' in self.summary_stats:
                print(f"  • Daily average: {self.summary_stats['daily_avg']} µg/m³")
            if 'cigarette_equivalent' in self.summary_stats:
                print(f"  • Last month equivalent: {self.summary_stats['cigarette_equivalent']} cigarettes")

        # Recommendation
        print("\nRecommendation:")
        if pm25 <= 5:
            print("  ✓ Safe for outdoor activities")
        elif pm25 <= 15:
            print("  ⚠ Limit prolonged exertion if sensitive")
        elif pm25 <= 25:
            print("  ⚠ Sensitive groups limit outdoor activities")
        elif pm25 <= 50:
            print("  ✗ Avoid outdoor activities")
        else:
            print("  ✗ Stay indoors, keep windows closed")

    def option_2_forecast(self):
        """Get hourly forecast."""
        self.print_header("HOURLY FORECAST")

        sensor_id = self.get_sensor_input()
        if sensor_id is None:
            return

        try:
            hours = int(input("\nForecast hours (default 24): ").strip() or "24")
            hours = min(max(hours, 1), 48)
        except:
            hours = 24

        df = self.get_file_data(sensor_id, hours=72)
        if len(df) < 24:
            print("\n  Insufficient data for forecast")
            return

        df = df.sort_values('datetime')
        historical = df['pm25'].values[-24:]

        # Simple forecast
        forecast = []
        for i in range(hours):
            if i < 24:
                pred = historical[i] * (1 + np.random.normal(0, 0.05))
            else:
                pred = forecast[-1] * 0.95 + np.mean(historical) * 0.05
            forecast.append(max(0, round(pred, 1)))

        print("\n" + "┌" + "─" * 58 + "┐")
        print("│                      24-HOUR FORECAST                        │")
        print("├" + "─" * 58 + "┤")
        now = datetime.now()
        for i in range(min(12, hours)):
            ftime = now + timedelta(hours=i+1)
            print(f"│ +{i+1:2d}h {ftime.strftime('%H:%M'):>5}  │  {forecast[i]:5.1f} µg/m³  │  {self.get_risk_level(forecast[i]):<25} │")
        print("└" + "─" * 58 + "┘")

        print(f"\nPeak: {max(forecast):.1f} µg/m³")
        print(f"Average: {np.mean(forecast):.1f} µg/m³")

    def option_3_compare(self):
        """Compare multiple sensors."""
        self.print_header("🔍 SENSOR COMPARISON")

        sensors_input = input("\nSensor IDs (e.g., 41,45,50): ").strip()
        try:
            sensor_ids = [int(s.strip()) for s in sensors_input.split(',')]
            sensor_ids = [s for s in sensor_ids if s in self.sensor_districts]
        except:
            print("  Invalid input")
            return

        if not sensor_ids:
            print("  No valid sensors selected")
            return

        print(f"\nComparing: {', '.join(map(str, sensor_ids))}")

        results = []
        for sid in sensor_ids:
            live = self.get_live_data(sid)
            if live:
                results.append({
                    'sensor': sid,
                    'district': self.sensor_districts[sid],
                    'pm25': live['pm25'],
                    'source': 'LIVE'
                })
            else:
                df = self.get_file_data(sid, hours=24)
                if len(df) > 0:
                    results.append({
                        'sensor': sid,
                        'district': self.sensor_districts[sid],
                        'pm25': df['pm25'].mean(),
                        'source': 'FILE'
                    })

        if not results:
            print("  No data available")
            return

        print("\n" + "┌" + "─" * 58 + "┐")
        print("│                        COMPARISON RESULTS                       │")
        print("├" + "─" * 58 + "┤")
        print("│ Sensor  District      PM2.5    Source    Risk Level             │")
        print("├" + "─" * 58 + "┤")
        for r in sorted(results, key=lambda x: x['pm25'], reverse=True):
            risk = self.get_risk_level(r['pm25'])
            print(f"│ {r['sensor']:<6} {r['district']:<12} {r['pm25']:<7.1f} {r['source']:<6}   {risk:<20} │")
        print("└" + "─" * 58 + "┘")

    def option_4_health_report(self):
        """Generate health report."""
        self.print_header("HEALTH RISK REPORT")

        sensors_input = input("\nSensor IDs (default: 41,45,50): ").strip()
        if sensors_input:
            try:
                sensor_ids = [int(s.strip()) for s in sensors_input.split(',')]
                sensor_ids = [s for s in sensor_ids if s in self.sensor_districts]
            except:
                sensor_ids = [41, 45, 50]
        else:
            sensor_ids = [41, 45, 50]

        print(f"\nAnalyzing sensors: {', '.join(map(str, sensor_ids))}")

        results = []
        for sid in sensor_ids:
            df = self.get_file_data(sid, hours=720)
            if len(df) > 0:
                mean_pm25 = df['pm25'].mean()
                results.append({
                    'sensor': sid,
                    'district': self.sensor_districts[sid],
                    'mean': mean_pm25,
                    'risk': self.get_risk_level(mean_pm25)
                })

        if results:
            print("\n" + "┌" + "─" * 58 + "┐")
            print("│                        HEALTH RISK SUMMARY                    │")
            print("├" + "─" * 58 + "┤")
            for r in results:
                print(f"│ Sensor {r['sensor']} ({r['district']}):")
                print(f"│   Mean PM2.5: {r['mean']:.1f} µg/m³ - {r['risk']}")
                if r['mean'] > 5:
                    risks = self.calculate_excess_risk(r['mean'])
                    print(f"│   Health impact: +{risks['mortality']}% mortality risk")
            print("└" + "─" * 58 + "┘")

            # Save report
            filename = f'health_report_{datetime.now().strftime("%Y%m%d")}.txt'
            with open(filename, 'w') as f:
                f.write("YEREVAN AIR QUALITY HEALTH REPORT\n")
                f.write("=" * 40 + "\n\n")
                for r in results:
                    f.write(f"Sensor {r['sensor']} ({r['district']}): {r['mean']:.1f} µg/m³ - {r['risk']}\n")
            print(f"\n Report saved: {filename}")

    def option_5_trend_analysis(self):
        """Analyze trends."""
        self.print_header("TREND ANALYSIS")

        sensor_id = self.get_sensor_input()
        if sensor_id is None:
            return

        try:
            days = int(input("\nDays to analyze (default 7): ").strip() or "7")
            days = min(days, 30)
        except:
            days = 7

        df = self.get_file_data(sensor_id, hours=days*24)
        if len(df) < 24:
            print("  Insufficient data")
            return

        df = df.sort_values('datetime')
        df['date'] = pd.to_datetime(df['datetime']).dt.date
        daily = df.groupby('date')['pm25'].mean().tail(days)

        print("\n" + "┌" + "─" * 58 + "┐")
        print("│                      DAILY AVERAGES                          │")
        print("├" + "─" * 58 + "┤")
        for date, val in daily.items():
            print(f"│ {date}  │  {val:5.1f} µg/m³  │  {self.get_risk_level(val):<25} │")
        print("└" + "─" * 58 + "┘")

        print(f"\nAverage: {daily.mean():.1f} µg/m³")
        print(f"Peak: {daily.max():.1f} µg/m³")

    def option_6_personalized_advice(self):
        """Get personalized advice."""
        self.print_header("========| PERSONALIZED HEALTH ADVICE |========")

        sensor_id = self.get_sensor_input()
        if sensor_id is None:
            return

        print("\nYour profile:")
        print("  1. Sensitive (asthma, respiratory)")
        print("  2. Athlete")
        print("  3. Elderly (65+)")
        print("  4. Parent")

        choice = input("\nChoice (1-4): ").strip()
        profile = self.profiles.get(choice, "sensitive")

        # Get current PM2.5
        live = self.get_live_data(sensor_id)
        if live:
            pm25 = live['pm25']
            source = "LIVE"
        else:
            df = self.get_file_data(sensor_id, hours=1)
            if len(df) > 0:
                pm25 = df['pm25'].iloc[0]
                source = "FILE"
            else:
                print("  No data available")
                return

        risk = self.get_risk_level(pm25)

        print("\n" + "┌" + "─" * 58 + "┐")
        print("│                      PERSONALIZED ADVICE                      │")
        print("├" + "─" * 58 + "┤")
        print(f"│ Current: {pm25:.1f} µg/m³ ({source}) - {risk}")
        print(f"│ Profile: {profile.title()}")
        print("├" + "─" * 58 + "┤")

        if profile == "sensitive":
            if pm25 <= 15:
                print("│  ✓ Generally safe, keep medication handy")
            elif pm25 <= 25:
                print("│  ⚠ Limit outdoor time, keep windows closed")
            else:
                print("│  ✗ Stay indoors, use air purifier")
        elif profile == "athlete":
            if pm25 <= 25:
                print("│  ✓ OK for light training")
            else:
                print("│  ✗ Train indoors today")
        elif profile == "elderly":
            if pm25 <= 15:
                print("│  ✓ Safe for short walks")
            else:
                print("│  ✗ Remain indoors")
        else:  # parent
            if pm25 <= 15:
                print("│  ✓ Safe for outdoor play")
            elif pm25 <= 25:
                print("│  ⚠ Limit outdoor play to 1 hour")
            else:
                print("│  ✗ Keep children indoors")
        print("└" + "─" * 58 + "┘")

    def option_7_draw_diagram(self):
        """Draw pollution diagram."""
        self.print_header("🖼️  POLLUTION DIAGRAM")

        year = self.get_year_input()
        if year is None:
            return

        month = self.get_month_input()
        if month is None:
            return

        print(f"\nGenerating diagram for {year}-{month:02d}...")

        try:
            df = self.file_loader.get_pm25_data(
                years=year,
                months=[month],
                include_metadata=True
            )
        except:
            print("  Error loading data")
            return

        if df is None or len(df) == 0:
            print("  No data available")
            return

        # Create visualization
        fig, axes = plt.subplots(2, 2, figsize=(15, 10))

        # Time series
        ax = axes[0, 0]
        df = df.sort_values('datetime')
        df['date'] = pd.to_datetime(df['datetime']).dt.date
        daily = df.groupby('date')['pm25'].mean()
        ax.plot(daily.index, daily.values, 'b-', linewidth=2)
        ax.axhline(5, color='g', ls='--', alpha=0.7, label='WHO Guideline (5)')
        ax.axhline(15, color='orange', ls='--', alpha=0.7, label='WHO Target (15)')
        ax.axhline(25, color='r', ls='--', alpha=0.7, label='Hazardous (25)')
        ax.set_title(f'Daily Average PM2.5 - {year}-{month:02d}')
        ax.set_ylabel('PM2.5 (µg/m³)')
        ax.legend()
        ax.grid(True, alpha=0.3)
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)

        # Distribution
        ax = axes[0, 1]
        ax.hist(df['pm25'], bins=50, edgecolor='black', alpha=0.7, color='steelblue')
        ax.axvline(df['pm25'].mean(), color='r', ls='--', label=f"Mean: {df['pm25'].mean():.1f}")
        ax.axvline(df['pm25'].median(), color='g', ls='--', label=f"Median: {df['pm25'].median():.1f}")
        ax.set_xlabel('PM2.5 (µg/m³)')
        ax.set_ylabel('Frequency')
        ax.set_title('Distribution of Measurements')
        ax.legend()
        ax.grid(True, alpha=0.3)

        # Hourly pattern
        ax = axes[1, 0]
        hourly = df.groupby(df['datetime'].dt.hour)['pm25'].mean()
        ax.plot(hourly.index, hourly.values, 'ro-', linewidth=2, markersize=6)
        ax.set_xlabel('Hour of Day')
        ax.set_ylabel('Average PM2.5')
        ax.set_title('Average Daily Pattern')
        ax.grid(True, alpha=0.3)

        # District comparison
        ax = axes[1, 1]
        if 'district_slug' in df.columns:
            district_map = {
                'avan': 'Avan', 'nor-nork': 'Nor Nork', 'ajapnyak': 'Ajapnyak',
                'arabkir': 'Arabkir', 'davtashen': 'Davtashen', 'erebuni': 'Erebuni',
                'shengavit': 'Shengavit', 'kentron': 'Kentron'
            }
            df['district'] = df['district_slug'].map(district_map).fillna(df['district_slug'])
            dist_avg = df.groupby('district')['pm25'].mean().sort_values(ascending=False).head(8)
            ax.barh(range(len(dist_avg)), dist_avg.values, color='coral')
            ax.set_yticks(range(len(dist_avg)))
            ax.set_yticklabels(dist_avg.index)
            ax.set_xlabel('Mean PM2.5 (µg/m³)')
            ax.set_title('Top Districts by Pollution')
            ax.axvline(5, color='g', ls='--', alpha=0.7, label='WHO Guideline')
            ax.legend()

        plt.tight_layout()

        filename = f'diagram_{year}_{month:02d}.png'
        plt.savefig(filename, dpi=150, bbox_inches='tight')
        plt.close()

        print(f"\n ========| Diagram saved: {filename} |========")
        print(f"\nStatistics for {year}-{month:02d}:")
        print(f"  Measurements: {len(df):,}")
        print(f"  Mean: {df['pm25'].mean():.1f} µg/m³")
        print(f"  Max: {df['pm25'].max():.1f} µg/m³")
        print(f"  Above WHO: {(df['pm25'] > 5).mean()*100:.1f}%")

    def option_8_list_sensors(self):
        """List all sensors."""
        self.print_header("AVAILABLE SENSORS")

        by_district = {}
        for sid, dist in self.sensor_districts.items():
            by_district.setdefault(dist, []).append(sid)

        print("\nSensors by district:")
        for dist in sorted(by_district):
            sensors = sorted(by_district[dist])
            print(f"  {dist:12}: {', '.join(map(str, sensors))}")

        print(f"\nTotal: {len(self.sensor_districts)} active sensors")

    def run(self):
        """Main program loop."""
        while True:
            self.print_menu()

            try:
                choice = input("\nEnter your choice (0-8): ").strip()

                if choice == "0":
                    print("\nThank you for using Yerevan Air Quality Tool. Stay healthy! 🌿")
                    break
                elif choice == "1":
                    self.option_1_current_quality()
                elif choice == "2":
                    self.option_2_forecast()
                elif choice == "3":
                    self.option_3_compare()
                elif choice == "4":
                    self.option_4_health_report()
                elif choice == "5":
                    self.option_5_trend_analysis()
                elif choice == "6":
                    self.option_6_personalized_advice()
                elif choice == "7":
                    self.option_7_draw_diagram()
                elif choice == "8":
                    self.option_8_list_sensors()
                else:
                    print("\n  Invalid choice. Please enter 0-8.")
                    input("\nPress Enter to continue...")
                    continue

                input("\nPress Enter to continue...")

            except KeyboardInterrupt:
                print("\n\nGoodbye! Stay healthy!")
                break

def main():
    """Start the application."""
    menu = AirQualityMenu()
    menu.run()

if __name__ == "__main__":
    main()