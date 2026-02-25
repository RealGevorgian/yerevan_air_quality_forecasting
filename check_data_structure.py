"""
Check the actual data directory structure.
"""
import os
from pathlib import Path


def check_structure():
    """Check what's in the data directory."""

    print("=" * 60)
    print("CHECKING DATA DIRECTORY STRUCTURE")
    print("=" * 60)

    # Check data/raw directory
    raw_dir = Path("data/raw")
    print(f"\n1. Checking: {raw_dir.absolute()}")

    if not raw_dir.exists():
        print(f"❌ Directory not found: {raw_dir}")
        return

    print(f"✅ Directory exists")

    # List contents of raw_dir
    print(f"\nContents of {raw_dir}:")
    for item in sorted(raw_dir.iterdir()):
        if item.is_dir():
            print(f"  📁 {item.name}/")
        else:
            size = item.stat().st_size / 1024  # KB
            print(f"  📄 {item.name} ({size:.1f} KB)")

    # Look for measurements folder
    measurements_candidates = [
        raw_dir / "measurements",
        raw_dir / "Measurement",
        raw_dir / "MEASUREMENTS",
        Path("data/measurements"),
        Path("data/raw/airquality_data/measurements"),
    ]

    print("\n2. Searching for measurements folder...")
    for path in measurements_candidates:
        if path.exists() and path.is_dir():
            print(f"✅ Found measurements at: {path}")
            # Count CSV files
            csv_files = list(path.glob("*.csv"))
            print(f"   Contains {len(csv_files)} CSV files")
            if csv_files:
                print(f"   First few files: {[f.name for f in csv_files[:5]]}")
            return path

    print("❌ Measurements folder not found in common locations")

    # Ask user where the files are
    print("\n3. Manual input needed")
    print("Where did you place the measurement files?")
    print("Common locations:")
    print("  - data/raw/measurements/")
    print("  - data/measurements/")
    print("  - data/raw/ (directly in raw)")

    return None


if __name__ == "__main__":
    measurements_path = check_structure()

    if measurements_path:
        print(f"\n✅ Use this path in your code: {measurements_path}")
    else:
        print("\n❌ Please move your measurement files to: data/raw/measurements/")