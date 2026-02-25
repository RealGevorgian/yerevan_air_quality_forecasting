"""
Debug script to identify CSV reading issues.
"""
import pandas as pd
from pathlib import Path


def debug_csv_file(file_path):
    """Debug a single CSV file."""

    print(f"\n{'=' * 60}")
    print(f"DEBUGGING FILE: {file_path}")
    print('=' * 60)

    # Check if file exists
    if not Path(file_path).exists():
        print(f"❌ File not found: {file_path}")
        return

    # Check file size
    file_size = Path(file_path).stat().st_size
    print(f"File size: {file_size:,} bytes ({file_size / 1024 / 1024:.2f} MB)")

    # Try to read just the first few rows
    print("\n1. Trying to read first 5 rows...")
    try:
        df_head = pd.read_csv(file_path, nrows=5)
        print(f"✅ Success! Found {len(df_head)} rows")
        print(f"Columns: {list(df_head.columns)}")
        print("\nFirst 2 rows:")
        print(df_head.head(2).to_string())
    except Exception as e:
        print(f"❌ Error reading first rows: {e}")

    # Try reading with different encodings
    print("\n2. Trying different encodings...")
    for encoding in ['utf-8', 'latin1', 'iso-8859-1', 'cp1252']:
        try:
            df = pd.read_csv(file_path, nrows=5, encoding=encoding)
            print(f"✅ Encoding {encoding} works!")
            break
        except:
            print(f"❌ Encoding {encoding} failed")

    # Check for column inconsistencies
    print("\n3. Analyzing column structure...")
    try:
        # Read without headers to see raw data
        df_raw = pd.read_csv(file_path, header=None, nrows=10)
        print(f"Raw data shape (first 10 rows): {df_raw.shape}")
        print(f"Number of columns in raw data: {len(df_raw.columns)}")

        # Check if first row might be header
        first_row = df_raw.iloc[0].tolist()
        print(f"First row values: {first_row[:10]}...")

        # Check if columns are consistent across rows
        col_counts = df_raw.apply(lambda x: x.count(), axis=1)
        print(f"Column counts per row: {col_counts.tolist()}")

        if col_counts.nunique() > 1:
            print("⚠️  Inconsistent number of columns across rows!")

    except Exception as e:
        print(f"❌ Error analyzing structure: {e}")

    # Try reading with error handling
    print("\n4. Trying to read with error handling...")
    try:
        df = pd.read_csv(file_path, on_bad_lines='skip', engine='python')
        print(f"✅ Read with bad lines skipped: {len(df)} rows")
    except Exception as e:
        print(f"❌ Error with bad lines skip: {e}")

    # Check for specific issues with the measurements_2025_01.csv file
    if '2025_01' in str(file_path):
        print("\n5. SPECIAL CHECK FOR JANUARY 2025 FILE")
        try:
            # Try reading with different parameters
            df_test = pd.read_csv(file_path,
                                  nrows=1000,
                                  on_bad_lines='warn',
                                  low_memory=False)
            print(f"✅ Read with low_memory=False: {len(df_test)} rows")

            # Check data types
            print("\nData types:")
            print(df_test.dtypes.head(10))

        except Exception as e:
            print(f"❌ Error in special check: {e}")


def main():
    """Main debug function."""

    # Path to your measurements directory
    measurements_dir = Path("data/raw/measurements")

    if not measurements_dir.exists():
        print(f"Measurements directory not found: {measurements_dir}")
        return

    # List all CSV files
    csv_files = list(measurements_dir.glob("*.csv"))
    print(f"Found {len(csv_files)} CSV files in {measurements_dir}")

    # Debug the January 2025 file specifically
    jan_2025_file = measurements_dir / "measurements_2025_01.csv"
    if jan_2025_file.exists():
        debug_csv_file(jan_2025_file)
    else:
        print(f"January 2025 file not found: {jan_2025_file}")

    # Also check a few other random files
    print("\n" + "=" * 60)
    print("CHECKING OTHER FILES FOR COMPARISON")
    print("=" * 60)

    for file in sorted(csv_files)[:3]:  # Check first 3 files
        if file.name != "measurements_2025_01.csv":
            debug_csv_file(file)
            print("\n" + "-" * 40)


if __name__ == "__main__":
    main()