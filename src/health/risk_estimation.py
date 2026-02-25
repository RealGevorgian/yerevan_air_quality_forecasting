"""
Health risk estimation based on PM2.5 concentrations.
Uses WHO and epidemiological literature for risk mapping.
"""
import pandas as pd
import numpy as np
from pathlib import Path
import sys

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.data.data_loader_final import AirQualityDataLoader

class HealthRiskEstimator:
    """
    Estimate health impacts of PM2.5 exposure using literature-based risk coefficients.
    """

    # WHO and epidemiological study parameters
    RISK_PARAMETERS = {
        'mortality': {
            'coefficient': 1.062,  # Relative risk per 10 µg/m³ increase
            'description': 'All-cause mortality (WHO 2021)',
            'unit': 'RR per 10 µg/m³'
        },
        'cardiovascular': {
            'coefficient': 1.11,
            'description': 'Cardiovascular hospital admissions',
            'unit': 'RR per 10 µg/m³'
        },
        'respiratory': {
            'coefficient': 1.08,
            'description': 'Respiratory hospital admissions',
            'unit': 'RR per 10 µg/m³'
        },
        'lung_cancer': {
            'coefficient': 1.09,
            'description': 'Lung cancer mortality',
            'unit': 'RR per 10 µg/m³'
        }
    }

    # WHO Air Quality Guidelines
    WHO_GUIDELINES = {
        'annual_mean': 5,      # µg/m³ annual mean
        '24h_mean': 15,         # µg/m³ 24-hour mean
        'interim_target_1': 35, # WHO Interim Target 1
        'interim_target_2': 25,
        'interim_target_3': 15,
        'interim_target_4': 10
    }

    def __init__(self, data_loader=None):
        self.loader = data_loader or AirQualityDataLoader(Path("data/raw"))
        self.results = {}

    def categorize_air_quality(self, pm25_value):
        """
        Categorize air quality based on WHO guidelines.
        """
        if pm25_value <= self.WHO_GUIDELINES['annual_mean']:
            return 'Good', 'green'
        elif pm25_value <= self.WHO_GUIDELINES['24h_mean']:
            return 'Moderate', 'yellow'
        elif pm25_value <= self.WHO_GUIDELINES['interim_target_4']:
            return 'Unhealthy for Sensitive Groups', 'orange'
        elif pm25_value <= self.WHO_GUIDELINES['interim_target_3']:
            return 'Unhealthy', 'red'
        elif pm25_value <= self.WHO_GUIDELINES['interim_target_2']:
            return 'Very Unhealthy', 'purple'
        else:
            return 'Hazardous', 'maroon'

    def calculate_excess_risk(self, pm25_value, baseline_risk=1.0):
        """
        Calculate excess health risk compared to WHO guideline.
        """
        if pm25_value <= self.WHO_GUIDELINES['annual_mean']:
            return {}

        excess = pm25_value - self.WHO_GUIDELINES['annual_mean']
        risk_factors = {}

        for outcome, params in self.RISK_PARAMETERS.items():
            # Relative risk calculation
            rr = params['coefficient'] ** (excess / 10)
            excess_risk = (rr - 1) * 100  # as percentage
            risk_factors[outcome] = {
                'relative_risk': round(rr, 3),
                'excess_risk_percent': round(excess_risk, 1),
                'description': params['description']
            }

        return risk_factors

    def estimate_population_impact(self, pm25_mean, population_size=1000000):
        """
        Estimate population-level health impacts.
        """
        excess = max(0, pm25_mean - self.WHO_GUIDELINES['annual_mean'])

        # Simplified impact estimation (based on literature)
        # These are illustrative coefficients - real studies use more complex models
        impacts = {
            'premature_deaths_per_year': round(population_size * 0.0001 * excess, 0),
            'hospital_admissions_per_year': round(population_size * 0.0002 * excess, 0),
            'asthma_emergency_visits': round(population_size * 0.00015 * excess, 0),
            'lost_work_days_per_year': round(population_size * 0.001 * excess, 0)
        }

        return impacts

    def analyze_sensor_health_risk(self, sensor_id=41, year=2025, month=1):
        """
        Analyze health risk for a specific sensor.
        """
        print(f"\nAnalyzing health risk for sensor {sensor_id}")

        # Load data
        df = self.loader.get_pm25_data(years=year, months=[month], sensors=[sensor_id])

        if len(df) == 0:
            print("No data found")
            return None

        # Calculate statistics
        mean_pm25 = df['pm25'].mean()
        max_pm25 = df['pm25'].max()
        pct_above_who = (df['pm25'] > self.WHO_GUIDELINES['annual_mean']).mean() * 100
        pct_hazardous = (df['pm25'] > self.WHO_GUIDELINES['interim_target_2']).mean() * 100

        # Get sensor location
        location = {}
        if 'district_slug' in df.columns:
            location = df[['latitude', 'longitude', 'district_slug']].iloc[0].to_dict()

        # Categorize
        category, color = self.categorize_air_quality(mean_pm25)

        # Calculate risks
        excess_risk = self.calculate_excess_risk(mean_pm25)

        results = {
            'sensor_id': sensor_id,
            'location': location,
            'mean_pm25': round(mean_pm25, 2),
            'max_pm25': round(max_pm25, 2),
            'air_quality_category': category,
            'category_color': color,
            'percent_above_who_guideline': round(pct_above_who, 1),
            'percent_hazardous': round(pct_hazardous, 1),
            'excess_health_risks': excess_risk,
            'data_points': len(df)
        }

        return results

    def generate_health_report(self, sensor_ids=[41, 45, 50], year=2025, month=1):
        """
        Generate comprehensive health risk report.
        """
        print("\n" + "="*70)
        print("HEALTH RISK ASSESSMENT REPORT")
        print("="*70)

        all_results = []
        for sensor_id in sensor_ids:
            result = self.analyze_sensor_health_risk(sensor_id, year, month)
            if result:
                all_results.append(result)

        if not all_results:
            print("No results to report")
            return None

        # Create summary DataFrame
        summary_data = []
        for r in all_results:
            district = r['location'].get('district_slug', 'Unknown') if r['location'] else 'Unknown'
            summary_data.append({
                'Sensor': r['sensor_id'],
                'District': district,
                'Mean PM2.5': r['mean_pm25'],
                'Category': r['air_quality_category'],
                '% Above WHO': r['percent_above_who_guideline'],
                'Data Points': r['data_points']
            })

        summary = pd.DataFrame(summary_data)

        print("\nSummary by Sensor:")
        print(summary.to_string(index=False))

        # Population impact estimate
        avg_pm25 = np.mean([r['mean_pm25'] for r in all_results])
        population_impact = self.estimate_population_impact(avg_pm25)

        print("\nEstimated Population Impact (per million residents):")
        print("-" * 50)
        for impact, value in population_impact.items():
            print(f"{impact.replace('_', ' ').title()}: {value:,.0f}")

        # Save report
        report_file = f'health_risk_report_{year}_{month:02d}.txt'
        with open(report_file, 'w') as f:
            f.write("="*70 + "\n")
            f.write("HEALTH RISK ASSESSMENT REPORT\n")
            f.write(f"Generated: {pd.Timestamp.now()}\n")
            f.write("="*70 + "\n\n")

            f.write(f"WHO Annual Guideline: {self.WHO_GUIDELINES['annual_mean']} µg/m³\n")
            f.write(f"WHO 24-hour Guideline: {self.WHO_GUIDELINES['24h_mean']} µg/m³\n\n")

            for r in all_results:
                district = r['location'].get('district_slug', 'Unknown') if r['location'] else 'Unknown'
                f.write(f"\nSensor {r['sensor_id']} ({district}):\n")
                f.write(f"  Mean PM2.5: {r['mean_pm25']} µg/m³\n")
                f.write(f"  Max PM2.5: {r['max_pm25']} µg/m³\n")
                f.write(f"  Air Quality: {r['air_quality_category']}\n")
                f.write(f"  % Above WHO: {r['percent_above_who_guideline']}%\n\n")

                if r['excess_health_risks']:
                    f.write("  Excess Health Risks:\n")
                    for outcome, risk in r['excess_health_risks'].items():
                        f.write(f"    {outcome.replace('_', ' ').title()}:\n")
                        f.write(f"      Relative Risk: {risk['relative_risk']}\n")
                        f.write(f"      Excess Risk: {risk['excess_risk_percent']}%\n")

            f.write("\n" + "="*70 + "\n")

        print(f"\n✅ Full report saved as '{report_file}'")
        return all_results

def main():
    """Run health risk analysis."""
    loader = AirQualityDataLoader(Path("data/raw"))
    estimator = HealthRiskEstimator(loader)

    # Analyze multiple sensors
    results = estimator.generate_health_report(
        sensor_ids=[41, 45, 50],
        year=2025,
        month=1
    )

if __name__ == "__main__":
    main()