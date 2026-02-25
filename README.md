# Yerevan Air Quality Prediction & Health Risk Assessment

A complete data science pipeline for predicting PM2.5 levels in Yerevan and estimating associated health risks using real-time monitoring data from airquality.am.

## Project Overview

This project provides a comprehensive solution for air quality monitoring in Yerevan, including:

- **Data ingestion** from airquality.am CSV archives
- **Data cleaning and harmonization** of 2.32GB of sensor data
- **Exploratory data analysis** with visualizations
- **Machine learning models** (XGBoost, Random Forest) for PM2.5 prediction
- **Health impact estimation** using WHO guidelines
- **Interactive CLI** for real-time air quality queries
- **Automated web scraping** for live data

## Key Achievements

- Processed 2.32GB of raw sensor data from 605 locations across Yerevan
- Achieved **93.2% prediction accuracy (R²=0.932)** with XGBoost models
- Identified significant health impacts: **3,600+ excess deaths per million residents** annually
- Created comprehensive health risk reports based on WHO guidelines

# Project Structure

```
yerevan_air_quality_project/
│
├── .venv/
│
├── data/
│   ├── processed/
│   └── raw/
│       ├── measurements/
│       └── sensors.csv
│
├── models/
│   ├── sensor_41_results.csv
│   ├── sensor_41_xgboost.pkl
│   ├── sensor_45_random_forest.pkl
│   ├── sensor_45_results.csv
│   ├── sensor_50_results.csv
│   └── sensor_50_xgboost.pkl
│
├── notebooks/
│   ├── 01_data_exploration.ipynb
│   └── 02_raw_data_exploration.ipynb
│
├── reports/
│
├── src/
│   ├── __init__.py
│   ├── config.py
│   ├── data/
│   │   ├── __init__.py
│   │   ├── data_cleaning.py
│   │   ├── data_loader.py
│   │   ├── data_loader_final.py
│   │   ├── data_loader_fixed.py
│   │   └── web_scraper.py
│   │
│   ├── health/
│   │   ├── __init__.py
│   │   └── risk_estimation.py
│   │
│   ├── models/
│   │   ├── __init__.py
│   │   ├── baseline_model.py
│   │   ├── predict.py
│   │   └── train_model.py
│   │
│   └── visualization/
│       ├── __init__.py
│       ├── config.py
│       └── plots.py
│
├── check_data_structure.py
├── city_daily_average.png
├── cli.py
├── debug_csv.py
├── district_comparison_2025_01.png
├── health_report_20260225.txt
├── health_risk_report_2025_01.txt
├── inspect_csv_structure.py
├── peek_file.py
├── pm25_distribution.png
├── pollution_diagram_2022_09.png
├── pollution_diagram_2025_12.png
├── prediction_insights_sensor_41.txt
├── prediction_insights_sensor_45.txt
├── prediction_insights_sensor_50.txt
├── predictions_sensor_41.png
├── predictions_sensor_45.png
├── predictions_sensor_50.png
├── quick_analysis.py
├── README.md
├── requirements.txt
├── sensor_41_analysis.png
├── test_final_loader.py
├── test_fixed_loader.py
└── test_imports.py
```


## Installation

### Prerequisites

- Python 3.8 or higher
- pip package manager

### Step 1: Clone the repository

```bash
git clone https://github.com/yourusername/yerevan_air_quality_project.git
cd yerevan_air_quality_project
```

### Step 2: Create a virtual environment

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### Step 3: Install dependencies

```bash
pip install -r requirements.txt
```

### Required packages

```
pandas>=1.5.0
numpy>=1.23.0
scipy>=1.9.0
matplotlib>=3.6.0
seaborn>=0.12.0
geopandas>=0.12.0
scikit-learn>=1.1.0
xgboost>=1.6.0
statsmodels>=0.13.0
pyarrow>=10.0.0
joblib>=1.2.0
tqdm>=4.64.0
requests>=2.28.0
beautifulsoup4>=4.11.0
jupyter>=1.0.0
ipykernel>=6.15.0
```

## Data Source

The project uses data from [airquality.am](https://airquality.am/data/). Download the following files and place them in `data/raw/measurements/`:

- `measurements_*.csv` files from 2019-2026
- `sensors.csv` for sensor metadata

## Usage

### Interactive CLI

The easiest way to use the program is through the interactive command-line interface:

```bash
python cli.py
```

You will see a menu with the following options:

```
╔══════════════════════════════════════════════════════════════════╗
║            YEREVAN AIR QUALITY & HEALTH ASSESSMENT               ║
╚══════════════════════════════════════════════════════════════════╝

 MAIN MENU:
  1. Check current air quality (LIVE)
  2. Get hourly forecast
  3. Compare air quality across locations
  4. Generate health risk report
  5. Analyze historical trends
  6. Get personalized health advice
  7. Draw air pollution diagram
  8. List all available sensors
  0. Exit
```

### Example: Check Current Air Quality

Select option 1 and enter a sensor ID:

```
Enter sensor ID: 41

Fetching data for sensor 41 (Avan)...

┌──────────────────────────────────────────────────────────┐
│                            RESULTS                       │
├──────────────────────────────────────────────────────────┤
│ Sensor:     41 (Avan)                                    |
│ Location:   Avan                                         |
│ Time:       2026-02-25 14:30:22 (LIVE)                   |
│ PM2.5:      54.8 µg/m³                                   |
│ Risk Level: Hazardous                                    |
└──────────────────────────────────────────────────────────┘

Health Impact:
  • Mortality: 34.6% excess risk
  • Cardiovascular: 67.4% excess risk
  • Respiratory: 46.2% excess risk

Recommendation:
  ✗ Stay indoors, keep windows closed
```

### Example: Generate Health Risk Report

Select option 4 to generate a comprehensive health report:

```
HEALTH RISK SUMMARY:

Sensor 41 (Avan): 53.4 µg/m³ - Hazardous
  Health impact: +34.6% mortality risk

Sensor 45 (Nor Nork): 12.4 µg/m³ - Moderate
  Health impact: +6.2% mortality risk

Sensor 50 (Ajapnyak): 58.3 µg/m³ - Hazardous
  Health impact: +38.1% mortality risk

========| Report saved: health_report_20260225.txt |========
```

### Running Individual Scripts

#### Quick data analysis

```bash
python quick_analysis.py
```

#### Train models for multiple sensors

```bash
python -m src.models.train_model
```

#### Generate visualizations

```bash
python -m src.visualization.plots
```

#### Test imports

```bash
python test_imports.py
```

## Model Performance

The project implements three modeling approaches:

| Model | Sensor 41 | Sensor 45 | Sensor 50 |
|-------|-----------|-----------|-----------|
| XGBoost | RMSE: 4.36, R²: 0.932 | RMSE: 1.63, R²: 0.905 | RMSE: 14.39, R²: 0.724 |
| Random Forest | RMSE: 4.97, R²: 0.912 | RMSE: 1.55, R²: 0.914 | RMSE: 14.57, R²: 0.717 |
| ARIMA | RMSE: 18.26, R²: -0.188 | RMSE: 5.14, R²: 0.062 | RMSE: 28.01, R²: -0.046 |

### Key Findings

- **XGBoost** consistently outperforms other models
- **Rolling averages** (3-hour, 6-hour) are the most important features
- Prediction accuracy varies by location (R² ranges from 0.72 to 0.93)

## Health Impact Assessment

Based on WHO guidelines and epidemiological literature:

- **WHO annual guideline**: 5 µg/m³
- **WHO 24-hour guideline**: 15 µg/m³
- **Excess mortality risk**: 6.2% per 10 µg/m³ above guideline

### Yerevan Summary (January 2025)

| District | Mean PM2.5 | Risk Level | Health Impact |
|----------|------------|------------|---------------|
| Avan | 53.4 µg/m³ | Hazardous | +47% excess mortality |
| Ajapnyak | 58.3 µg/m³ | Hazardous | +53% excess mortality |
| Nor Nork | 12.4 µg/m³ | Moderate | +9% excess mortality |

**Population impact per million residents:**
- 3,636 premature deaths per year
- 7,273 hospital admissions per year
- 36,363 lost work days per year

## Generated Outputs

Running the program creates several output files:

### Prediction Plots
- `predictions_sensor_41.png` - 6-panel visualization for sensor 41
- `predictions_sensor_45.png` - Model performance plots
- `predictions_sensor_50.png` - Prediction vs actual comparisons

### Health Reports
- `health_report_YYYYMMDD.txt` - Daily health risk summary
- `health_risk_report_2025_01.txt` - Comprehensive monthly report

### Diagrams
- `pollution_diagram_2025_12.png` - Time series, distribution, hourly patterns
- `city_daily_average.png` - City-wide daily averages
- `district_comparison_2025_01.png` - Comparison across districts
- `sensor_41_analysis.png` - Detailed sensor analysis

### Model Files
- `models/sensor_41_xgboost.pkl` - Trained XGBoost model
- `models/sensor_45_random_forest.pkl` - Trained Random Forest
- `models/sensor_50_xgboost.pkl` - Trained XGBoost model
- `models/sensor_*_results.csv` - Performance metrics

## Troubleshooting

### Common Issues

1. **Import errors**: Run `python test_imports.py` to diagnose path issues
2. **No data available**: Ensure CSV files are in `data/raw/measurements/`
3. **Web scraping fails**: Check internet connection; falls back to file data

### Debug Scripts

- `debug_csv.py` - Diagnose CSV file structure
- `inspect_csv_structure.py` - Analyze CSV format
- `peek_file.py` - View raw file contents
- `test_final_loader.py` - Test data loader
- `test_fixed_loader.py` - Test fixed loader

## License

This project is for educational and research purposes. Data source: airquality.am

## Acknowledgments

- Data provided by [airquality.am](https://airquality.am)
- WHO guidelines for air quality and health impact assessment
- AUA Air Quality Lab for the research opportunity

