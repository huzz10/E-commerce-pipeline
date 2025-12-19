"""
Script to run BigQuery analytics queries and save results
"""
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from gcp.bigquery_client import BigQueryClient
from utils.logger import setup_logger
from utils.config_loader import load_config, get_gcp_config

logger = setup_logger(__name__)


def run_analytics_queries():
    """Run all analytics queries and display results."""
    config = load_config()
    gcp_config = get_gcp_config(config)
    
    project_id = gcp_config.get('project_id', 'your-gcp-project-id')
    dataset_id = gcp_config.get('dataset_id', 'ecommerce_analytics')
    
    bq_client = BigQueryClient(project_id=project_id, dataset_id=dataset_id)
    
    # Read SQL files
    sql_dir = Path(__file__).parent
    
    queries = {
        'daily_revenue': sql_dir / 'daily_revenue.sql',
        'category_growth': sql_dir / 'category_growth.sql',
        'demand_vs_actual': sql_dir / 'demand_vs_actual.sql',
        'top_selling_products': sql_dir / 'top_selling_products.sql'
    }
    
    results = {}
    
    for query_name, sql_file in queries.items():
        logger.info(f"Running query: {query_name}")
        
        try:
            # Read SQL file
            with open(sql_file, 'r') as f:
                sql = f.read()
            
            # Replace placeholders
            sql = sql.replace('{project_id}', project_id)
            sql = sql.replace('{dataset_id}', dataset_id)
            
            # Execute query
            df = bq_client.query(sql)
            
            results[query_name] = df
            
            logger.info(f"Query {query_name} completed: {len(df)} rows")
            print(f"\n=== {query_name.upper().replace('_', ' ')} ===")
            print(df.head(10).to_string())
            
        except Exception as e:
            logger.error(f"Error running query {query_name}: {e}")
            results[query_name] = None
    
    return results


if __name__ == "__main__":
    run_analytics_queries()

