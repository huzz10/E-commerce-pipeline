"""
ML Model Inference for Demand Prediction

This module loads trained models and generates predictions for next-day and next-week demand.
"""
import pandas as pd
import numpy as np
import pickle
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional

import sys
sys.path.append(str(Path(__file__).parent.parent))

from utils.logger import setup_logger
from utils.config_loader import load_config
from ml.feature_engineering import FeatureEngineer
from gcp.bigquery_client import BigQueryClient

logger = setup_logger(__name__)


class DemandPredictionInference:
    """
    Inference engine for demand prediction models.
    """
    
    def __init__(self, model_dir: str = None):
        """
        Initialize inference engine.
        
        Args:
            model_dir: Directory containing trained models
        """
        config = load_config()
        ml_config = config.get('ml', {})
        
        self.model_dir = Path(model_dir or ml_config.get('model_dir', 'models'))
        self.feature_engineer = FeatureEngineer()
        
        self.models = {}
        self.model_metadata = {}
        
        logger.info(f"DemandPredictionInference initialized, model_dir: {self.model_dir}")
    
    def load_model(self, model_name: str, model_version: str = None) -> Dict[str, Any]:
        """
        Load a trained model and its metadata.
        
        Args:
            model_name: Name of the model (e.g., 'linear_regression', 'xgboost')
            model_version: Model version (if None, loads latest)
        
        Returns:
            Dictionary with model and metadata
        """
        # Find model files
        if model_version:
            model_path = self.model_dir / f"{model_name}_{model_version}.pkl"
            metadata_path = self.model_dir / f"{model_name}_{model_version}_metadata.json"
        else:
            # Find latest version
            model_files = list(self.model_dir.glob(f"{model_name}_*.pkl"))
            if not model_files:
                raise FileNotFoundError(f"No model found for {model_name}")
            
            model_path = sorted(model_files)[-1]
            metadata_path = model_path.with_suffix('.json').with_name(
                model_path.stem + '_metadata.json'
            )
        
        # Load model
        with open(model_path, 'rb') as f:
            model = pickle.load(f)
        
        # Load metadata
        with open(metadata_path, 'r') as f:
            metadata = json.load(f)
        
        logger.info(f"Loaded model: {model_name}, version: {metadata['model_version']}")
        
        return {
            'model': model,
            'metadata': metadata
        }
    
    def prepare_prediction_data(self, historical_df: pd.DataFrame,
                               prediction_date: datetime,
                               products: List[str] = None) -> pd.DataFrame:
        """
        Prepare data for prediction by creating features up to prediction_date.
        
        Args:
            historical_df: Historical sales data
            prediction_date: Date to predict for
            products: List of product IDs (if None, predicts for all)
        
        Returns:
            DataFrame ready for prediction
        """
        logger.info(f"Preparing prediction data for {prediction_date.date()}")
        
        # Filter historical data up to prediction_date
        historical_df = historical_df[historical_df['event_date'] < prediction_date]
        
        # Filter products if specified
        if products:
            historical_df = historical_df[historical_df['product_id'].isin(products)]
        
        # Get unique products
        unique_products = historical_df['product_id'].unique()
        
        # Create prediction rows (one per product)
        prediction_rows = []
        for product_id in unique_products:
            product_data = historical_df[historical_df['product_id'] == product_id].copy()
            
            # Create a row for prediction_date
            last_row = product_data.iloc[-1].copy()
            last_row['event_date'] = prediction_date
            last_row['daily_quantity'] = 0  # Placeholder, will be predicted
            
            prediction_rows.append(last_row)
        
        prediction_df = pd.DataFrame(prediction_rows)
        
        # Feature engineering
        prediction_df = self.feature_engineer.engineer_features(
            prediction_df, 
            target_col='daily_quantity'
        )
        
        return prediction_df
    
    def predict(self, model_name: str, prediction_df: pd.DataFrame,
               model_version: str = None) -> pd.DataFrame:
        """
        Generate predictions using a trained model.
        
        Args:
            model_name: Name of the model to use
            prediction_df: DataFrame with features
            model_version: Model version (if None, uses latest)
        
        Returns:
            DataFrame with predictions
        """
        logger.info(f"Generating predictions using {model_name}")
        
        # Load model if not already loaded
        if model_name not in self.models:
            model_data = self.load_model(model_name, model_version)
            self.models[model_name] = model_data['model']
            self.model_metadata[model_name] = model_data['metadata']
        
        model = self.models[model_name]
        metadata = self.model_metadata[model_name]
        
        # Get feature columns
        feature_cols = metadata['feature_columns']
        
        # Prepare features
        X = prediction_df[feature_cols]
        
        # Generate predictions
        predictions = model.predict(X)
        
        # Add predictions to DataFrame
        result_df = prediction_df[['event_date', 'product_id', 'category']].copy()
        result_df['predicted_demand'] = predictions
        result_df['model_name'] = model_name
        result_df['model_version'] = metadata['model_version']
        
        return result_df
    
    def predict_next_day(self, historical_df: pd.DataFrame,
                        model_name: str = 'xgboost',
                        products: List[str] = None) -> pd.DataFrame:
        """
        Predict demand for next day.
        
        Args:
            historical_df: Historical sales data
            model_name: Model to use for prediction
            products: List of product IDs (if None, predicts for all)
        
        Returns:
            DataFrame with next-day predictions
        """
        prediction_date = datetime.now() + timedelta(days=1)
        
        prediction_df = self.prepare_prediction_data(
            historical_df, prediction_date, products
        )
        
        result_df = self.predict(model_name, prediction_df)
        result_df['prediction_horizon'] = 'next_day'
        result_df['prediction_date'] = prediction_date.date()
        
        return result_df
    
    def predict_next_week(self, historical_df: pd.DataFrame,
                         model_name: str = 'xgboost',
                         products: List[str] = None) -> pd.DataFrame:
        """
        Predict demand for next week (7 days ahead).
        
        Args:
            historical_df: Historical sales data
            model_name: Model to use for prediction
            products: List of product IDs (if None, predicts for all)
        
        Returns:
            DataFrame with next-week predictions
        """
        prediction_date = datetime.now() + timedelta(days=7)
        
        prediction_df = self.prepare_prediction_data(
            historical_df, prediction_date, products
        )
        
        result_df = self.predict(model_name, prediction_df)
        result_df['prediction_horizon'] = 'next_week'
        result_df['prediction_date'] = prediction_date.date()
        
        return result_df
    
    def save_predictions_to_bigquery(self, predictions_df: pd.DataFrame):
        """
        Save predictions to BigQuery.
        
        Args:
            predictions_df: DataFrame with predictions
        """
        logger.info("Saving predictions to BigQuery")
        
        # Prepare DataFrame for BigQuery
        bq_df = predictions_df.copy()
        bq_df['created_at'] = datetime.now()
        
        # Ensure correct column names
        bq_df = bq_df.rename(columns={
            'prediction_date': 'prediction_date',
            'product_id': 'product_id',
            'category': 'category',
            'predicted_demand': 'predicted_demand',
            'prediction_horizon': 'prediction_horizon',
            'model_name': 'model_name',
            'model_version': 'model_version'
        })
        
        # Select only required columns
        bq_df = bq_df[[
            'prediction_date', 'product_id', 'category', 'predicted_demand',
            'prediction_horizon', 'model_name', 'model_version', 'created_at'
        ]]
        
        # Convert prediction_date to datetime if needed
        if bq_df['prediction_date'].dtype == 'object':
            bq_df['prediction_date'] = pd.to_datetime(bq_df['prediction_date'])
        
        # Save to BigQuery
        bq_client = BigQueryClient()
        bq_client.insert_dataframe('demand_predictions', bq_df)
        
        logger.info(f"Saved {len(bq_df)} predictions to BigQuery")


def main():
    """Main function to run inference."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate Demand Predictions')
    parser.add_argument('--data', type=str, required=True, help='Path to historical data')
    parser.add_argument('--model', type=str, default='xgboost', help='Model name')
    parser.add_argument('--horizon', type=str, choices=['next_day', 'next_week'], 
                       default='next_day', help='Prediction horizon')
    parser.add_argument('--model-dir', type=str, help='Model directory')
    parser.add_argument('--save-bq', action='store_true', help='Save to BigQuery')
    
    args = parser.parse_args()
    
    # Load historical data
    if args.data.endswith('.parquet'):
        historical_df = pd.read_parquet(args.data)
    else:
        historical_df = pd.read_csv(args.data)
    
    historical_df['event_date'] = pd.to_datetime(historical_df['event_date'])
    
    # Initialize inference
    inference = DemandPredictionInference(model_dir=args.model_dir)
    
    # Generate predictions
    if args.horizon == 'next_day':
        predictions = inference.predict_next_day(historical_df, args.model)
    else:
        predictions = inference.predict_next_week(historical_df, args.model)
    
    print(f"\nPredictions ({args.horizon}):")
    print(predictions.head(10))
    
    # Save to BigQuery if requested
    if args.save_bq:
        inference.save_predictions_to_bigquery(predictions)


if __name__ == "__main__":
    main()

