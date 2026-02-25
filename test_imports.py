"""
Test imports to diagnose the problem.
"""
import sys
from pathlib import Path

print("=" * 50)
print("IMPORT TEST")
print("=" * 50)

project_root = Path(__file__).parent
print(f"Project root: {project_root}")

# Add to path
sys.path.insert(0, str(project_root))
print(f"\nPython path includes: {project_root}")

# Try to import
try:
    print("\nTrying: from src.data.data_loader_final import AirQualityDataLoader")
    from src.data.data_loader_final import AirQualityDataLoader
    print("✅ SUCCESS: AirQualityDataLoader imported")
except ImportError as e:
    print(f"❌ FAILED: {e}")

try:
    print("\nTrying: from src.data.web_scraper import AirQualityScraper")
    from src.data.web_scraper import AirQualityScraper
    print("✅ SUCCESS: AirQualityScraper imported")
except ImportError as e:
    print(f"❌ FAILED: {e}")

print("\n" + "=" * 50)
print("Current sys.path:")
for i, path in enumerate(sys.path):
    print(f"  {i}: {path}")
print("=" * 50)