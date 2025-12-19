"""
Configuration loader utility
"""
import yaml
import os
from pathlib import Path
from typing import Dict, Any


def load_config(config_path: str = None) -> Dict[str, Any]:
    """
    Load configuration from YAML file.
    
    Args:
        config_path: Path to config file. Defaults to configs/config.yaml
    
    Returns:
        Configuration dictionary
    """
    if config_path is None:
        config_path = Path(__file__).parent.parent / "configs" / "config.yaml"
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Replace environment variables if present
    config = _replace_env_vars(config)
    
    return config


def _replace_env_vars(config: Dict) -> Dict:
    """Recursively replace environment variables in config."""
    if isinstance(config, dict):
        return {k: _replace_env_vars(v) for k, v in config.items()}
    elif isinstance(config, list):
        return [_replace_env_vars(item) for item in config]
    elif isinstance(config, str):
        # Replace {var_name} with environment variable if exists
        if config.startswith('{') and config.endswith('}'):
            var_name = config[1:-1]
            return os.getenv(var_name, config)
        return config
    return config


def get_gcp_config(config: Dict) -> Dict:
    """Extract GCP-specific configuration."""
    return config.get('gcp', {})


def get_kafka_config(config: Dict) -> Dict:
    """Extract Kafka-specific configuration."""
    return config.get('kafka', {})


def get_spark_config(config: Dict) -> Dict:
    """Extract Spark-specific configuration."""
    return config.get('spark', {})

