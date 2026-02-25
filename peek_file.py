"""
Quick peek at the beginning of the CSV file.
"""
from pathlib import Path


def peek_file(file_path, num_lines=20):
    """Display first few lines of a file."""

    print(f"\n{'=' * 60}")
    print(f"PEEKING AT: {file_path.name}")
    print('=' * 60)

    with open(file_path, 'r', encoding='utf-8') as f:
        for i in range(num_lines):
            try:
                line = f.readline().strip()
                if not line:
                    break
                print(f"{i + 1:3d}: {line[:100]}{'...' if len(line) > 100 else ''}")
            except Exception as e:
                print(f"Error at line {i + 1}: {e}")
                break


def main():
    """Peek at measurement files."""

    measurements_dir = Path("data/raw/measurements")

    # Peek at January 2025
    jan_file = measurements_dir / "measurements_2025_01.csv"
    if jan_file.exists():
        peek_file(jan_file)

    # Also peek at a much older file to compare
    old_file = measurements_dir / "measurements_2019_01.csv"
    if old_file.exists():
        peek_file(old_file)


if __name__ == "__main__":
    main()