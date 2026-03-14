import os
import time
import logging
import pyarrow as pa
from pyiceberg.catalog import load_catalog

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    catalog_uri = os.getenv('CATALOG_URI', 'http://localhost:8181/')
    s3_endpoint = os.getenv('S3_ENDPOINT', 'http://localhost:9000')
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', 'minio')
    aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'minio123')
    
    logger.info("Connecting to Iceberg Rest Catalog...")
    catalog = load_catalog(
        "default",
        **{
            "type": "rest",
            "uri": catalog_uri,
            "s3.endpoint": s3_endpoint,
            "s3.access-key-id": aws_access_key,
            "s3.secret-access-key": aws_secret_key,
        }
    )
    
    try:
        catalog.create_namespace("demo")
        logger.info("Created namespace 'demo'")
    except Exception:
        logger.info("Namespace 'demo' already exists")
        
    schema = pa.schema([
        ('id', pa.int64()),
        ('data', pa.string())
    ])
    
    try:
        table = catalog.create_table("demo.events", schema=schema)
        logger.info("Created table 'demo.events'")
    except Exception:
        table = catalog.load_table("demo.events")
        logger.info("Loaded table 'demo.events'")
        
    max_iterations = int(os.getenv('MAX_ITERATIONS', '10'))
    logger.info(f"Starting data generation loop for {max_iterations} iterations. Press Ctrl+C to exit early.")
    try:
        for iteration in range(max_iterations):
            i = iteration * 3
            # Create a small pyarrow table
            df = pa.Table.from_arrays(
                [
                    [i, i+1, i+2], 
                    [f"data_{i}", f"data_{i+1}", f"data_{i+2}"]
                ],
                schema=schema
            )
            table.append(df)
            logger.info(f"Appended 3 records to demo.events. Total iterations: {iteration + 1}/{max_iterations}")
            
            # Delete some old records occasionally to show delete metrics (if supported by Python append)
            # PyIceberg currently has limited delete support, so we stick to appends to show activity
            
            if iteration < max_iterations - 1:
                time.sleep(15)
                
        logger.info("Data generation completed successfully.")
    except KeyboardInterrupt:
        logger.info("Data generation stopped early by user.")

if __name__ == "__main__":
    main()
