"""
ML Model Training for Demand Prediction

This module trains baseline (Linear Regression) and advanced (XGBoost) models
for predicting product demand.
"""
import pandas as pd
import numpy as np
import pickle
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Tuple

from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
import xgboost as xgb

import sys
sys.path.append(str(Path(__file__).parent.parent))

from utils.logger import setup_logger
from utils.config_loader import load_config
from ml.feature_engineering import FeatureEngineer

logger = setup_logger(__name__)


class DemandPredictionTrainer:
    """
    Trainer for demand prediction models.
    """
    
    def __init__(self, model_dir: str = None):
        """
        Initialize trainer.
        
        Args:
            model_dir: Directory to save models
        """
        config = load_config()
        ml_config = config.get('ml', {})
        
        self.model_dir = Path(model_dir or ml_config.get('model_dir', 'models'))
        self.model_dir.mkdir(parents=True, exist_ok=True)
        
        self.feature_engineer = FeatureEngineer()
        
        logger.info(f"DemandPredictionTrainer initialized, model_dir: {self.model_dir}")
    
    def load_training_data(self, data_path: str) -> pd.DataFrame:
        """
        Load training data from CSV or Parquet.
        
        Args:
            data_path: Path to data file
        
        Returns:
            Training DataFrame
        """
        logger.info(f"Loading training data from: {data_path}")
        
        if data_path.endswith('.parquet'):
            df = pd.read_parquet(data_path)
        else:
            df = pd.read_csv(data_path)
        
        logger.info(f"Loaded {len(df)} rows")
        return df
    
    def prepare_data(self, df: pd.DataFrame, 
                    target_col: str = "daily_quantity") -> Tuple[pd.DataFrame, pd.Series, List[str]]:
        """
        Prepare data for training: feature engineering and train/test split.
        
        Args:
            df: Input DataFrame
            target_col: Target column name
        
        Returns:
            Tuple of (X_train, X_test, y_train, y_test, feature_names)
        """
        logger.info("Preparing training data")
        
        # Feature engineering
        df_features = self.feature_engineer.engineer_features(df, target_col=target_col)
        
        # Get feature columns
        feature_cols = self.feature_engineer.get_feature_columns(df_features)
        
        # Prepare X and y
        X = df_features[feature_cols]
        y = df_features[target_col]
        
        # Train/test split (80/20)
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, shuffle=False
        )
        
        logger.info(f"Training set: {len(X_train)} samples, Test set: {len(X_test)} samples")
        logger.info(f"Features: {len(feature_cols)}")
        
        return X_train, X_test, y_train, y_test, feature_cols
    
    def train_linear_regression(self, X_train: pd.DataFrame, y_train: pd.Series,
                                X_test: pd.DataFrame, y_test: pd.Series) -> Dict[str, Any]:
        """
        Train Linear Regression baseline model.
        
        Args:
            X_train: Training features
            y_train: Training target
            X_test: Test features
            y_test: Test target
        
        Returns:
            Dictionary with model and metrics
        """
        logger.info("Training Linear Regression model")
        
        model = LinearRegression()
        model.fit(X_train, y_train)
        
        # Predictions
        y_train_pred = model.predict(X_train)
        y_test_pred = model.predict(X_test)
        
        # Metrics
        train_mae = mean_absolute_error(y_train, y_train_pred)
        test_mae = mean_absolute_error(y_test, y_test_pred)
        train_rmse = np.sqrt(mean_squared_error(y_train, y_train_pred))
        test_rmse = np.sqrt(mean_squared_error(y_test, y_test_pred))
        train_r2 = r2_score(y_train, y_train_pred)
        test_r2 = r2_score(y_test, y_test_pred)
        
        metrics = {
            'train_mae': float(train_mae),
            'test_mae': float(test_mae),
            'train_rmse': float(train_rmse),
            'test_rmse': float(test_rmse),
            'train_r2': float(train_r2),
            'test_r2': float(test_r2)
        }
        
        logger.info(f"Linear Regression - Test MAE: {test_mae:.2f}, Test R2: {test_r2:.3f}")
        
        return {
            'model': model,
            'metrics': metrics,
            'model_name': 'linear_regression'
        }
    
    def train_xgboost(self, X_train: pd.DataFrame, y_train: pd.Series,
                     X_test: pd.DataFrame, y_test: pd.Series,
                     params: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Train XGBoost advanced model.
        
        Args:
            X_train: Training features
            y_train: Training target
            X_test: Test features
            y_test: Test target
            params: XGBoost parameters
        
        Returns:
            Dictionary with model and metrics
        """
        logger.info("Training XGBoost model")
        
        default_params = {
            'n_estimators': 100,
            'max_depth': 6,
            'learning_rate': 0.1,
            'subsample': 0.8,
            'colsample_bytree': 0.8,
            'random_state': 42
        }
        
        if params:
            default_params.update(params)
        
        model = xgb.XGBRegressor(**default_params)
        model.fit(X_train, y_train)
        
        # Predictions
        y_train_pred = model.predict(X_train)
        y_test_pred = model.predict(X_test)
        
        # Metrics
        train_mae = mean_absolute_error(y_train, y_train_pred)
        test_mae = mean_absolute_error(y_test, y_test_pred)
        train_rmse = np.sqrt(mean_squared_error(y_train, y_train_pred))
        test_rmse = np.sqrt(mean_squared_error(y_test, y_test_pred))
        train_r2 = r2_score(y_train, y_train_pred)
        test_r2 = r2_score(y_test, y_test_pred)
        
        metrics = {
            'train_mae': float(train_mae),
            'test_mae': float(test_mae),
            'train_rmse': float(train_rmse),
            'test_rmse': float(test_rmse),
            'train_r2': float(train_r2),
            'test_r2': float(test_r2)
        }
        
        logger.info(f"XGBoost - Test MAE: {test_mae:.2f}, Test R2: {test_r2:.3f}")
        
        return {
            'model': model,
            'metrics': metrics,
            'model_name': 'xgboost',
            'params': default_params
        }
    
    def save_model(self, model_result: Dict[str, Any], feature_cols: List[str],
                  model_version: str = None):
        """
        Save trained model and metadata.
        
        Args:
            model_result: Dictionary with model and metrics
            feature_cols: List of feature column names
            model_version: Model version string
        """
        model_name = model_result['model_name']
        model_version = model_version or datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save model
        model_path = self.model_dir / f"{model_name}_{model_version}.pkl"
        with open(model_path, 'wb') as f:
            pickle.dump(model_result['model'], f)
        
        # Save metadata
        metadata = {
            'model_name': model_name,
            'model_version': model_version,
            'feature_columns': feature_cols,
            'metrics': model_result['metrics'],
            'trained_at': datetime.now().isoformat()
        }
        
        if 'params' in model_result:
            metadata['params'] = model_result['params']
        
        metadata_path = self.model_dir / f"{model_name}_{model_version}_metadata.json"
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        logger.info(f"Saved model to {model_path}")
        logger.info(f"Saved metadata to {metadata_path}")
    
    def train_all_models(self, data_path: str, target_col: str = "daily_quantity"):
        """
        Train all models (baseline and advanced).
        
        Args:
            data_path: Path to training data
            target_col: Target column name
        """
        logger.info("Starting model training pipeline")
        
        # Load data
        df = self.load_training_data(data_path)
        
        # Prepare data
        X_train, X_test, y_train, y_test, feature_cols = self.prepare_data(df, target_col)
        
        # Train Linear Regression
        lr_result = self.train_linear_regression(X_train, y_train, X_test, y_test)
        self.save_model(lr_result, feature_cols, model_version="v1")
        
        # Train XGBoost
        xgb_result = self.train_xgboost(X_train, y_train, X_test, y_test)
        self.save_model(xgb_result, feature_cols, model_version="v1")
        
        logger.info("Model training pipeline completed")


def main():
    """Main function to run training."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Train Demand Prediction Models')
    parser.add_argument('--data', type=str, required=True, help='Path to training data')
    parser.add_argument('--target', type=str, default='daily_quantity', help='Target column name')
    parser.add_argument('--model-dir', type=str, help='Model directory')
    
    args = parser.parse_args()
    
    trainer = DemandPredictionTrainer(model_dir=args.model_dir)
    trainer.train_all_models(args.data, args.target)


if __name__ == "__main__":
    main()

