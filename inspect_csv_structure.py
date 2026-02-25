"""
Inspect the actual structure of the CSV files.
"""
import pandas as pd
from pathlib import Path


def inspect_file(file_path, num_rows=20):
    """Inspect a CSV file to understand its structure."""

    print(f"\n{'=' * 60}")
    print(f"INSPECTING: {file_path.name}")
    print('=' * 60)

    # Read raw lines first
    print("\n1. RAW FIRST 5 LINES:")
    print('-' * 40)
    with open(file_path, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            if i >= 5:
                break
            print(f"Line {i}: {line.strip()}")

    # Try reading with different approaches
    print("\n2. READING WITH PANDAS (raw, no header):")
    print('-' * 40)
    try:
        df_raw = pd.read_csv(file_path, header=None, nrows=10)
        print(f"Shape: {df_raw.shape}")
        print("First 5 rows:")
        print(df_raw.head(5).to_string())
    except Exception as e:
        print(f"Error: {e}")

    # Check if it might be a different delimiter
    print("\n3. TRYING DIFFERENT DELIMITERS:")
    print('-' * 40)
    for delimiter in [',', ';', '\t', '|']:
        try:
            df = pd.read_csv(file_path, delimiter=delimiter, nrows=5)
            print(f"Delimiter '{delimiter}': {df.shape} - Columns: {list(df.columns)}")
        except:
            print(f"Delimiter '{delimiter}': Failed")

    # Look for any numeric columns
    print("\n4. CHECKING FOR NUMERIC DATA:")
    print('-' * 40)
    try:
        # Try to read with pandas and see data types
        df = pd.read_csv(file_path, nrows=100)
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        print(f"Numeric columns: {numeric_cols}")

        if numeric_cols:
            print("\nSample numeric data:")
            print(df[numeric_cols].head(10).to_string())
        else:
            # Try to convert columns to numeric
            print("No numeric columns detected. Attempting conversion...")
            for col in df.columns:
                try:
                    converted = pd.to_numeric(df[col], errors='coerce')
                    if converted.notna().sum() > 0:
                        print(f"Column '{col}' can be converted to numeric")
                        print(f"  Sample values: {converted.dropna().head(5).tolist()}")
                except:
                    pass
    except Exception as e:
        print(f"Error: {e}")


def main():
    """Main inspection function."""

    measurements_dir = Path("data/raw/measurements")

    # Inspect January 2025 file
    jan_file = measurements_dir / "measurements_2025_01.csv"
    if jan_file.exists():
        inspect_file(jan_file)

    # Also check a few other files to see if structure is consistent
    print("\n\n" + "=" * 60)
    print("CHECKING OTHER FILES FOR CONSISTENCY")
    print("=" * 60)

    other_files = [
        measurements_dir / "measurements_2024_12.csv",
        measurements_dir / "measurements_2025_02.csv",
        measurements_dir / "measurements_2023_01.csv"
    ]

    for file in other_files:
        if file.exists():
            try:
                df = pd.read_csv(file, nrows=5)
                print(f"\n{file.name}: {df.shape} - Columns: {list(df.columns)}")
            except Exception as e:
                print(f"\n{file.name}: Error - {e}")


if __name__ == "__main__":
    main()