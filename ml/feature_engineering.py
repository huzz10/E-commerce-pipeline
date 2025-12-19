"""
Feature Engineering for Demand Prediction

This module creates features for ML models:
- Historical sales
- Rolling averages
- Day of week
- Seasonality
- Category features
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Any

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from utils.logger import setup_logger

logger = setup_logger(__name__)


class FeatureEngineer:
    """
    Feature engineering for demand prediction models.
    """
    
    def __init__(self):
        """Initialize feature engineer."""
        logger.info("FeatureEngineer initialized")
    
    def create_temporal_features(self, df: pd.DataFrame, date_col: str = "event_date") -> pd.DataFrame:
        """
        Create temporal features from date column.
        
        Args:
            df: Input DataFrame
            date_col: Name of date column
        
        Returns:
            DataFrame with temporal features
        """
        df = df.copy()
        df[date_col] = pd.to_datetime(df[date_col])
        
        # Day of week (0=Monday, 6=Sunday)
        df['day_of_week'] = df[date_col].dt.dayofweek
        
        # Day of month
        df['day_of_month'] = df[date_col].dt.day
        
        # Month
        df['month'] = df[date_col].dt.month
        
        # Quarter
        df['quarter'] = df[date_col].dt.quarter
        
        # Is weekend
        df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)
        
        # Is month end
        df['is_month_end'] = df[date_col].dt.is_month_end.astype(int)
        
        # Week of year
        df['week_of_year'] = df[date_col].dt.isocalendar().week
        
        logger.info("Created temporal features")
        return df
    
    def create_rolling_features(self, df: pd.DataFrame, 
                               group_cols: List[str],
                               value_col: str,
                               date_col: str = "event_date") -> pd.DataFrame:
        """
        Create rolling average features.
        
        Args:
            df: Input DataFrame
            group_cols: Columns to group by (e.g., ['product_id'])
            value_col: Column to calculate rolling averages for
            date_col: Date column name
        
        Returns:
            DataFrame with rolling features
        """
        df = df.copy()
        df[date_col] = pd.to_datetime(df[date_col])
        df = df.sort_values([*group_cols, date_col])
        
        # Rolling averages: 7 days, 14 days, 30 days
        for window in [7, 14, 30]:
            df[f'rolling_avg_{window}d'] = df.groupby(group_cols)[value_col].transform(
                lambda x: x.rolling(window=window, min_periods=1).mean()
            )
            
            df[f'rolling_std_{window}d'] = df.groupby(group_cols)[value_col].transform(
                lambda x: x.rolling(window=window, min_periods=1).std().fillna(0)
            )
        
        # Lag features (previous day, week)
        df['lag_1d'] = df.groupby(group_cols)[value_col].shift(1)
        df['lag_7d'] = df.groupby(group_cols)[value_col].shift(7)
        
        logger.info("Created rolling features")
        return df
    
    def create_category_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create category-based features.
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame with category features
        """
        df = df.copy()
        
        # Category encoding (one-hot or label encoding)
        category_dummies = pd.get_dummies(df['category'], prefix='category')
        df = pd.concat([df, category_dummies], axis=1)
        
        # Category average demand (historical)
        category_avg = df.groupby('category')['daily_quantity'].mean().to_dict()
        df['category_avg_demand'] = df['category'].map(category_avg)
        
        logger.info("Created category features")
        return df
    
    def create_product_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create product-specific features.
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame with product features
        """
        df = df.copy()
        
        # Product average price
        product_avg_price = df.groupby('product_id')['avg_price'].mean().to_dict()
        df['product_avg_price'] = df['product_id'].map(product_avg_price)
        
        # Product total historical sales
        product_total_sales = df.groupby('product_id')['daily_quantity'].sum().to_dict()
        df['product_total_sales'] = df['product_id'].map(product_total_sales)
        
        # Product days since first sale
        product_first_sale = df.groupby('product_id')['event_date'].min().to_dict()
        df['days_since_first_sale'] = (
            df['event_date'] - df['product_id'].map(product_first_sale)
        ).dt.days
        
        logger.info("Created product features")
        return df
    
    def engineer_features(self, df: pd.DataFrame, 
                         target_col: str = "daily_quantity",
                         group_cols: List[str] = None) -> pd.DataFrame:
        """
        Complete feature engineering pipeline.
        
        Args:
            df: Input DataFrame with sales data
            target_col: Target column for prediction
            group_cols: Columns to group by for rolling features
        
        Returns:
            DataFrame with all engineered features
        """
        logger.info("Starting feature engineering pipeline")
        
        group_cols = group_cols or ['product_id']
        
        # Temporal features
        df = self.create_temporal_features(df)
        
        # Rolling features
        df = self.create_rolling_features(df, group_cols, target_col)
        
        # Category features
        if 'category' in df.columns:
            df = self.create_category_features(df)
        
        # Product features
        df = self.create_product_features(df)
        
        # Fill NaN values
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        df[numeric_cols] = df[numeric_cols].fillna(0)
        
        logger.info("Feature engineering pipeline completed")
        return df
    
    def get_feature_columns(self, df: pd.DataFrame, 
                           exclude_cols: List[str] = None) -> List[str]:
        """
        Get list of feature columns (excluding target and metadata).
        
        Args:
            df: Input DataFrame
            exclude_cols: Columns to exclude
        
        Returns:
            List of feature column names
        """
        exclude_cols = exclude_cols or [
            'event_date', 'product_id', 'category', 
            'daily_quantity', 'daily_revenue', 'daily_orders'
        ]
        
        feature_cols = [col for col in df.columns if col not in exclude_cols]
        
        # Only numeric features
        feature_cols = [col for col in feature_cols if df[col].dtype in [np.int64, np.float64]]
        
        return feature_cols


def main():
    """Main function for testing."""
    # Sample data
    dates = pd.date_range('2024-01-01', periods=100, freq='D')
    df = pd.DataFrame({
        'event_date': dates,
        'product_id': ['P001'] * 100,
        'category': ['Electronics'] * 100,
        'daily_quantity': np.random.randint(10, 100, 100),
        'avg_price': [299.99] * 100
    })
    
    engineer = FeatureEngineer()
    df_features = engineer.engineer_features(df)
    
    print(f"Original columns: {df.columns.tolist()}")
    print(f"Feature columns: {engineer.get_feature_columns(df_features)}")
    print(f"\nSample features:\n{df_features.head()}")


if __name__ == "__main__":
    main()

