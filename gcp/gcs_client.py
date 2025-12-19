"""
GCS Client for E-commerce Data Lake

This module handles GCS operations for Bronze/Silver/Gold layers.
"""
from google.cloud import storage
from typing import List, Optional
import os

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from utils.logger import setup_logger
from utils.config_loader import load_config, get_gcp_config

logger = setup_logger(__name__)


class GCSClient:
    """
    Client for GCS operations.
    """
    
    def __init__(self, project_id: str = None, bucket_name: str = None):
        """
        Initialize GCS client.
        
        Args:
            project_id: GCP project ID
            bucket_name: GCS bucket name
        """
        config = load_config()
        gcp_config = get_gcp_config(config)
        
        self.project_id = project_id or gcp_config.get('project_id', 'your-gcp-project-id')
        self.bucket_name = bucket_name or gcp_config.get('bucket_name', 'ecommerce-data-lake')
        
        self.client = storage.Client(project=self.project_id)
        
        # Ensure bucket exists
        self._create_bucket_if_not_exists()
        
        logger.info(f"GCS client initialized for bucket: {self.bucket_name}")
    
    def _create_bucket_if_not_exists(self):
        """Create GCS bucket if it doesn't exist."""
        try:
            self.client.get_bucket(self.bucket_name)
            logger.info(f"Bucket {self.bucket_name} already exists")
        except Exception:
            bucket = self.client.create_bucket(self.bucket_name, location="US")
            logger.info(f"Created bucket {self.bucket_name}")
    
    def upload_file(self, local_path: str, gcs_path: str):
        """
        Upload a file to GCS.
        
        Args:
            local_path: Local file path
            gcs_path: GCS destination path
        """
        bucket = self.client.bucket(self.bucket_name)
        blob = bucket.blob(gcs_path)
        
        blob.upload_from_filename(local_path)
        logger.info(f"Uploaded {local_path} to gs://{self.bucket_name}/{gcs_path}")
    
    def download_file(self, gcs_path: str, local_path: str):
        """
        Download a file from GCS.
        
        Args:
            gcs_path: GCS source path
            local_path: Local destination path
        """
        bucket = self.client.bucket(self.bucket_name)
        blob = bucket.blob(gcs_path)
        
        blob.download_to_filename(local_path)
        logger.info(f"Downloaded gs://{self.bucket_name}/{gcs_path} to {local_path}")
    
    def list_files(self, prefix: str = "") -> List[str]:
        """
        List files in bucket with given prefix.
        
        Args:
            prefix: Prefix to filter files
        
        Returns:
            List of file paths
        """
        bucket = self.client.bucket(self.bucket_name)
        blobs = bucket.list_blobs(prefix=prefix)
        
        files = [blob.name for blob in blobs]
        logger.info(f"Found {len(files)} files with prefix {prefix}")
        
        return files
    
    def delete_file(self, gcs_path: str):
        """
        Delete a file from GCS.
        
        Args:
            gcs_path: GCS file path
        """
        bucket = self.client.bucket(self.bucket_name)
        blob = bucket.blob(gcs_path)
        blob.delete()
        
        logger.info(f"Deleted gs://{self.bucket_name}/{gcs_path}")


def main():
    """Main function for testing."""
    client = GCSClient()
    files = client.list_files(prefix="bronze/")
    print(f"Files in bronze layer: {files}")


if __name__ == "__main__":
    main()

