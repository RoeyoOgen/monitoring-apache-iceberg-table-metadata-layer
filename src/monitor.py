import os
import logging
import pyiceberg
from prometheus_client import start_http_server, Gauge
from pyiceberg.catalog import load_catalog

logger = logging.getLogger(__name__)

# Define Snapshot Metrics
SNAPSHOT_METRICS = {
    'added_data_files': Gauge('iceberg_snapshot_added_data_files', 'Added data files', ['table_name']),
    'added_records': Gauge('iceberg_snapshot_added_records', 'Added records', ['table_name']),
    'total_records': Gauge('iceberg_snapshot_total_records', 'Total records', ['table_name']),
    'total_data_files': Gauge('iceberg_snapshot_total_data_files', 'Total data files', ['table_name']),
    'total_delete_files': Gauge('iceberg_snapshot_total_delete_files', 'Total delete files', ['table_name']),
    'added_files_size': Gauge('iceberg_snapshot_added_files_size', 'Added files size', ['table_name']),
    'total_files_size': Gauge('iceberg_snapshot_total_files_size', 'Total files size', ['table_name']),
    'added_position_deletes': Gauge('iceberg_snapshot_added_position_deletes', 'Added position deletes', ['table_name'])
}

# Define File Metrics
FILE_METRICS = {
    'avg_record_count': Gauge('iceberg_files_avg_record_count', 'Avg record count in files', ['table_name']),
    'max_record_count': Gauge('iceberg_files_max_record_count', 'Max record count in files', ['table_name']),
    'min_record_count': Gauge('iceberg_files_min_record_count', 'Min record count in files', ['table_name']),
    'avg_file_size': Gauge('iceberg_files_avg_file_size', 'Avg file size', ['table_name']),
    'max_file_size': Gauge('iceberg_files_max_file_size', 'Max file size', ['table_name']),
    'min_file_size': Gauge('iceberg_files_min_file_size', 'Min file size', ['table_name']),
}

def safe_float(val, default=0.0):
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default

def start_server(port):
    start_http_server(port)
    logger.info(f"Prometheus metrics server started on port {port}")
    logger.info(f"PyIceberg version: {pyiceberg.__version__}")

def update_metrics():
    catalog_uri = os.getenv('CATALOG_URI', 'http://localhost:8181/')
    s3_endpoint = os.getenv('S3_ENDPOINT', 'http://localhost:9000')
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', 'minio')
    aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'minio123')
    
    try:
        catalog = load_catalog(
            "default",
            **{
                "type": "rest",
                "uri": catalog_uri,
                "s3.endpoint": s3_endpoint,
                "s3.access-key-id": aws_access_key,
                "s3.secret-access-key": aws_secret_key,
                "s3.path-style-access": "true"
            }
        )
        
        namespaces = catalog.list_namespaces()
        logger.info(f"Found namespaces: {namespaces}")
        
        for namespace_tuple in namespaces:
            namespace = namespace_tuple[0] if isinstance(namespace_tuple, tuple) else namespace_tuple
            if namespace in ['system', 'information_schema']:
                continue
                
            tables = catalog.list_tables(namespace)
            logger.info(f"Found {len(tables)} tables in namespace {namespace}")
            
            for table_identifier in tables:
                if isinstance(table_identifier, tuple):
                    table_name = ".".join(table_identifier)
                else:
                    table_name = table_identifier
                
                logger.info(f"Processing table {table_name}")
                
                table = catalog.load_table(table_identifier)
                logger.info(f"Loaded table {table_name}")
                
                # Snapshot metrics
                snapshot = table.current_snapshot()
                if snapshot and snapshot.summary:
                    logger.info(f"Updating snapshot metrics for {table_name}")
                    for metric_key, gauge in SNAPSHOT_METRICS.items():
                        summary_key = metric_key.replace('_', '-')
                        val = snapshot.summary.get(summary_key, 0)
                        gauge.labels(table_name=table_name).set(safe_float(val))
                else:
                    logger.info(f"No current snapshot found for {table_name}")
                        
                # File metrics via pyiceberg inspect (req pyiceberg >= 0.6.0)
                try:
                    logger.info(f"Checking inspect for {table_name}")
                    if hasattr(table, 'inspect'):
                        logger.info(f"Table {table_name} has inspect. Checking for files()")
                        files_df = table.inspect.files().to_pandas()
                        if not files_df.empty:
                            logger.info(f"Updating file metrics for {table_name} ({len(files_df)} files)")
                            FILE_METRICS['avg_record_count'].labels(table_name=table_name).set(safe_float(files_df['record_count'].mean()))
                            FILE_METRICS['max_record_count'].labels(table_name=table_name).set(safe_float(files_df['record_count'].max()))
                            FILE_METRICS['min_record_count'].labels(table_name=table_name).set(safe_float(files_df['record_count'].min()))
                            FILE_METRICS['avg_file_size'].labels(table_name=table_name).set(safe_float(files_df['file_size_in_bytes'].mean()))
                            FILE_METRICS['max_file_size'].labels(table_name=table_name).set(safe_float(files_df['file_size_in_bytes'].max()))
                            FILE_METRICS['min_file_size'].labels(table_name=table_name).set(safe_float(files_df['file_size_in_bytes'].min()))
                        else:
                            logger.warning(f"No data files found for table {table_name}")
                    else:
                        logger.error(f"Table {table_name} does NOT have inspect attribute. PyIceberg version: {pyiceberg.__version__}")
                except Exception as e:
                    logger.error(f"Could not inspect files for {table_name}: {e}")
                    
    except Exception as e:
        logger.error(f"Error updating metrics: {e}")
