"""
Data cleaning and preprocessing functions for air quality data.
"""
import pandas as pd
import numpy as np
from typing import Optional, Dict, List, Tuple
import logging
from scipy import stats

logger = logging.getLogger(__name__)


class AirQualityCleaner:
    """Clean and preprocess air quality data."""

    def __init__(self):
        self.cleaning_log = []
        self.pm25_column = None

    def detect_pm25_column(self, df: pd.DataFrame) -> str:
        """
        Automatically detect the PM2.5 column in the dataframe.

        Args:
            df: Input dataframe

        Returns:
            Name of the PM2.5 column
        """
        # Common PM2.5 column names
        pm25_patterns = ['pm25', 'pm2.5', 'pm2_5', 'pm 2.5', 'pm']

        for col in df.columns:
            col_lower = col.lower().strip()
            if any(pattern in col_lower for pattern in pm25_patterns):
                # Prefer columns with just pm25, not pm10
                if '10' not in col_lower:
                    self.pm25_column = col
                    logger.info(f"========| Detected PM2.5 column: '{col}' |========")
                    return col

        raise ValueError(f"Could not detect PM2.5 column. Available columns: {df.columns.tolist()}")

    def basic_clean(self, df: pd.DataFrame, pm25_col: Optional[str] = None) -> pd.DataFrame:
        """
        Perform basic cleaning operations.

        Args:
            df: Input dataframe
            pm25_col: Name of PM2.5 column (auto-detected if None)
        """
        df_clean = df.copy()

        # Detect PM2.5 column if not provided
        if pm25_col is None:
            pm25_col = self.detect_pm25_column(df_clean)

        # Initial stats
        initial_rows = len(df_clean)
        logger.info(f"Starting basic cleaning for {initial_rows} rows")

        # Remove duplicate rows
        before_dedup = len(df_clean)
        df_clean = df_clean.drop_duplicates()
        dup_removed = before_dedup - len(df_clean)
        if dup_removed > 0:
            self.cleaning_log.append(f"Removed {dup_removed} duplicate rows")
            logger.info(f"  Removed {dup_removed} duplicates")

        # Check for and handle negative PM2.5 values
        if pm25_col in df_clean.columns:
            neg_count = (df_clean[pm25_col] < 0).sum()
            if neg_count > 0:
                # Replace negative values with NaN
                df_clean.loc[df_clean[pm25_col] < 0, pm25_col] = np.nan
                self.cleaning_log.append(f"Replaced {neg_count} negative PM2.5 values with NaN")
                logger.info(f"  Replaced {neg_count} negative values with NaN")

        # Ensure date column is datetime
        if 'date' in df_clean.columns:
            df_clean['date'] = pd.to_datetime(df_clean['date'])

        # Sort by date if available
        if 'date' in df_clean.columns:
            df_clean = df_clean.sort_values('date').reset_index(drop=True)

        logger.info(f"========| Basic cleaning complete. {len(df_clean)} rows remaining |========")
        return df_clean

    def handle_outliers(self,
                        df: pd.DataFrame,
                        pm25_col: str,
                        method: str = 'iqr',
                        threshold: float = 3.0) -> pd.DataFrame:
        """
        Detect and handle outliers in PM2.5 data.

        Args:
            df: Input dataframe
            pm25_col: Name of PM2.5 column
            method: 'iqr' or 'zscore'
            threshold: Threshold for outlier detection
        """
        df_out = df.copy()

        # Remove any remaining NaN values for calculation
        valid_data = df_out[pm25_col].dropna()

        if len(valid_data) == 0:
            logger.warning("No valid PM2.5 data for outlier detection")
            return df_out

        if method == 'iqr':
            Q1 = valid_data.quantile(0.25)
            Q3 = valid_data.quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - threshold * IQR
            upper_bound = Q3 + threshold * IQR

            outlier_mask = (df_out[pm25_col] < lower_bound) | (df_out[pm25_col] > upper_bound)

        elif method == 'zscore':
            z_scores = np.abs(stats.zscore(valid_data))
            outlier_indices = valid_data.index[z_scores > threshold]
            outlier_mask = df_out.index.isin(outlier_indices)

        else:
            raise ValueError(f"Unknown method: {method}")

        outlier_count = outlier_mask.sum()
        if outlier_count > 0:
            # Set outliers to NaN
            df_out.loc[outlier_mask, pm25_col] = np.nan
            self.cleaning_log.append(f"Removed {outlier_count} outliers using {method} (threshold={threshold})")
            logger.info(f"  Removed {outlier_count} outliers")

        return df_out

    def handle_missing_values(self,
                              df: pd.DataFrame,
                              pm25_col: str,
                              method: str = 'interpolate_time',
                              max_consecutive: Optional[int] = 7) -> pd.DataFrame:
        """
        Handle missing values in PM2.5 data.

        Args:
            df: Input dataframe (must have 'date' column for time-based methods)
            pm25_col: Name of PM2.5 column
            method: 'interpolate_time', 'interpolate_linear', 'ffill', 'bfill', 'mean'
            max_consecutive: Maximum number of consecutive missing values to fill
        """
        df_filled = df.copy()

        if 'date' not in df_filled.columns:
            logger.warning("No date column found for time-based interpolation")
            method = 'interpolate_linear'

        missing_before = df_filled[pm25_col].isna().sum()
        missing_pct = (missing_before / len(df_filled)) * 100

        logger.info(f"Missing values before: {missing_before} ({missing_pct:.1f}%)")

        if missing_before == 0:
            logger.info("No missing values to handle")
            return df_filled

        if method == 'interpolate_time' and 'date' in df_filled.columns:
            # Convert dates to numeric for time-based interpolation
            df_filled = df_filled.set_index('date')
            df_filled[pm25_col] = df_filled[pm25_col].interpolate(method='time')
            df_filled = df_filled.reset_index()

        elif method == 'interpolate_linear':
            df_filled[pm25_col] = df_filled[pm25_col].interpolate(method='linear', limit=max_consecutive)

        elif method == 'ffill':
            df_filled[pm25_col] = df_filled[pm25_col].fillna(method='ffill', limit=max_consecutive)

        elif method == 'bfill':
            df_filled[pm25_col] = df_filled[pm25_col].fillna(method='bfill', limit=max_consecutive)

        elif method == 'mean':
            mean_val = df_filled[pm25_col].mean()
            df_filled[pm25_col] = df_filled[pm25_col].fillna(mean_val)
            self.cleaning_log.append(f"Filled missing values with mean: {mean_val:.1f}")

        missing_after = df_filled[pm25_col].isna().sum()
        filled = missing_before - missing_after

        self.cleaning_log.append(f"Filled {filled} missing values using {method}")
        logger.info(f"Missing values after: {missing_after} (filled {filled})")

        return df_filled

    def clean_pipeline(self,
                       df: pd.DataFrame,
                       pm25_col: Optional[str] = None,
                       remove_outliers: bool = True,
                       handle_missing: bool = True,
                       outlier_method: str = 'iqr',
                       missing_method: str = 'interpolate_time') -> pd.DataFrame:
        """
        Run the complete cleaning pipeline.

        Args:
            df: Input dataframe
            pm25_col: Name of PM2.5 column
            remove_outliers: Whether to remove outliers
            handle_missing: Whether to handle missing values
            outlier_method: Method for outlier detection
            missing_method: Method for missing value handling
        """
        logger.info("=" * 50)
        logger.info("Starting complete cleaning pipeline")

        # Reset cleaning log
        self.cleaning_log = []

        # Step 1: Basic cleaning
        df_clean = self.basic_clean(df, pm25_col)

        # Get the PM2.5 column name (now stored in self.pm25_column)
        if pm25_col is None:
            pm25_col = self.pm25_column

        # Step 2: Remove outliers (optional)
        if remove_outliers:
            df_clean = self.handle_outliers(df_clean, pm25_col, method=outlier_method)

        # Step 3: Handle missing values (optional)
        if handle_missing:
            df_clean = self.handle_missing_values(df_clean, pm25_col, method=missing_method)

        # Final stats
        final_missing = df_clean[pm25_col].isna().sum()
        final_pct = (final_missing / len(df_clean)) * 100

        logger.info(f"\nCleaning Summary:")
        logger.info(f"  Total rows: {len(df_clean)}")
        logger.info(f"  Final missing values: {final_missing} ({final_pct:.1f}%)")
        logger.info("=" * 50)

        return df_clean

    def get_cleaning_summary(self) -> str:
        """Return a summary of cleaning operations performed."""
        if not self.cleaning_log:
            return "No cleaning operations performed yet."

        summary = "Cleaning Operations Performed:\n" + "-" * 30 + "\n"
        for i, op in enumerate(self.cleaning_log, 1):
            summary += f"{i}. {op}\n"
        return summary