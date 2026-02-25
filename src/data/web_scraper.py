"""
Web scraper for real-time air quality data from airquality.am
Targets the Yerevan city page with clean, structured information.
"""
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import re
import json

class AirQualityScraper:
    """
    Scrapes real-time air quality data from airquality.am Yerevan page.
    """

    def __init__(self):
        self.url = "https://airquality.am/en/air-quality/yerevan"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        self.sensor_data = {}

    def get_current_readings(self):
        """
        Scrape current air quality readings for Yerevan.
        Returns a list of sensor readings with detailed information.
        """
        print("  Fetching real-time data from airquality.am...")

        try:
            response = requests.get(self.url, headers=self.headers, timeout=10)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')

            readings = []

            # Extract main city-wide reading
            city_reading = self._extract_city_reading(soup)
            if city_reading:
                readings.append(city_reading)

            # Extract individual sensor cards
            sensor_cards = soup.find_all('div', class_='sensor-card')
            for card in sensor_cards:
                sensor_data = self._extract_sensor_data(card)
                if sensor_data:
                    readings.append(sensor_data)

            # Extract summary statistics
            summary = self._extract_summary_stats(soup)
            if summary:
                self.summary_stats = summary

            return readings

        except requests.RequestException as e:
            print(f"  Error fetching data: {e}")
            return []
        except Exception as e:
            print(f"  Error parsing data: {e}")
            return []

    def _extract_city_reading(self, soup):
        """
        Extract the main city-wide air quality reading.
        """
        try:
            # Find the main pollution level indicator
            level_elem = soup.find('div', class_='pollution-level')
            if level_elem:
                level_text = level_elem.text.strip()

                # Find the numeric value
                value_elem = soup.find('span', class_='pollution-value')
                pm25 = None
                if value_elem:
                    value_text = value_elem.text.strip()
                    pm25_match = re.search(r'(\d+\.?\d*)', value_text)
                    pm25 = float(pm25_match.group(1)) if pm25_match else None

                return {
                    'sensor_id': 0,  # City-wide reading
                    'location': 'Yerevan City',
                    'pm25': pm25,
                    'risk_level': level_text,
                    'timestamp': datetime.now(),
                    'source': 'web_scrape',
                    'type': 'city_average'
                }
        except:
            return None

    def _extract_sensor_data(self, card):
        """
        Extract data from an individual sensor card.
        """
        try:
            # Extract location
            location_elem = card.find('div', class_='location')
            location = location_elem.text.strip() if location_elem else "Unknown"

            # Extract PM2.5 value
            value_elem = card.find('span', class_='value')
            pm25 = None
            if value_elem:
                value_text = value_elem.text.strip()
                pm25_match = re.search(r'(\d+\.?\d*)', value_text)
                pm25 = float(pm25_match.group(1)) if pm25_match else None

            # Extract risk level/color
            risk_elem = card.find('span', class_='risk')
            risk_level = risk_elem.text.strip() if risk_elem else "Unknown"

            # Try to extract sensor ID from data attribute or link
            sensor_link = card.find('a', href=True)
            sensor_id = None
            if sensor_link:
                href = sensor_link['href']
                sensor_id_match = re.search(r'/sensor/(\d+)', href)
                sensor_id = int(sensor_id_match.group(1)) if sensor_id_match else None

            if pm25:
                return {
                    'sensor_id': sensor_id,
                    'location': location,
                    'pm25': pm25,
                    'risk_level': risk_level,
                    'timestamp': datetime.now(),
                    'source': 'web_scrape',
                    'type': 'sensor'
                }
        except:
            pass
        return None

    def _extract_summary_stats(self, soup):
        """
        Extract summary statistics from the page.
        """
        summary = {}
        try:
            # Daily average
            daily_elem = soup.find('div', class_='daily-average')
            if daily_elem:
                daily_text = daily_elem.text.strip()
                daily_match = re.search(r'(\d+\.?\d*)', daily_text)
                summary['daily_avg'] = float(daily_match.group(1)) if daily_match else None

            # Yearly average
            yearly_elem = soup.find('div', class_='yearly-average')
            if yearly_elem:
                yearly_text = yearly_elem.text.strip()
                yearly_match = re.search(r'(\d+\.?\d*)', yearly_text)
                summary['yearly_avg'] = float(yearly_match.group(1)) if yearly_match else None

            # Days exceeding WHO
            days_elem = soup.find('div', class_='exceeding-days')
            if days_elem:
                days_text = days_elem.text.strip()
                days_match = re.search(r'(\d+)', days_text)
                summary['days_exceeding_who'] = int(days_match.group(1)) if days_match else None

            # Cigarette equivalent
            cig_elem = soup.find('div', class_='cigarette-equivalent')
            if cig_elem:
                cig_text = cig_elem.text.strip()
                cig_match = re.search(r'(\d+)', cig_text)
                summary['cigarette_equivalent'] = int(cig_match.group(1)) if cig_match else None

            return summary
        except:
            return None

    def get_sensor_reading(self, sensor_id):
        """
        Get current reading for a specific sensor.
        """
        all_readings = self.get_current_readings()

        for reading in all_readings:
            if reading.get('sensor_id') == sensor_id:
                return reading

        # Return city average if sensor not found
        for reading in all_readings:
            if reading.get('type') == 'city_average':
                return reading

        return None

    def get_all_readings_dict(self):
        """
        Get all readings as a dictionary keyed by sensor_id.
        """
        readings = self.get_current_readings()
        result = {}
        for r in readings:
            if r.get('sensor_id'):
                result[r['sensor_id']] = r
        return result

    def get_summary_stats(self):
        """
        Get summary statistics for Yerevan.
        """
        if not hasattr(self, 'summary_stats'):
            self.get_current_readings()
        return getattr(self, 'summary_stats', {})

    def get_city_reading(self):
        """
        Get the city-wide average reading.
        """
        readings = self.get_current_readings()
        for r in readings:
            if r.get('type') == 'city_average':
                return r
        return None

# Simple test if run directly
if __name__ == "__main__":
    scraper = AirQualityScraper()
    readings = scraper.get_current_readings()

    print(f"\nFound {len(readings)} readings:")
    for r in readings:
        print(f"  {r.get('type', 'unknown')}: {r.get('location', 'N/A')} - {r.get('pm25', 'N/A')} µg/m³")

    summary = scraper.get_summary_stats()
    if summary:
        print(f"\nSummary Statistics:")
        for key, value in summary.items():
            print(f"  {key}: {value}")