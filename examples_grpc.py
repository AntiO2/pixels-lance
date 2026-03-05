"""
Example usage of gRPC client with Pixels Lance parser and storage
"""

from pixels_lance.grpc_fetcher import PixelsGrpcFetcher
from pixels_lance.fetcher import RowRecordBinaryExtractor
from pixels_lance.parser import DataParser
from pixels_lance.storage import LanceDBStore
from pixels_lance.config import ConfigManager
from pixels_lance.logger import get_logger

logger = get_logger(__name__)


def example_grpc_polling():
    """Example: Poll data from PixelsPollingService and store in LanceDB"""
    
    # Load configuration
    config = ConfigManager("config/config.yaml").get()
    
    # Initialize gRPC client
    grpc_fetcher = PixelsGrpcFetcher(
        host=config.rpc.grpc_host or "localhost",
        port=config.rpc.grpc_port,
        timeout=config.rpc.timeout,
    )
    
    # Initialize parser for customer table
    parser = DataParser(
        schema_path="config/schema_hybench.yaml",
        table_name="customer"
    )
    
    # Initialize LanceDB storage
    store = LanceDBStore(
        db_path=config.lancedb.db_path,
        mode=config.lancedb.mode,
    )
    
    try:
        # Connect to gRPC service
        grpc_fetcher.connect()
        
        # Poll events from Pixels
        logger.info("Polling events from PixelsPollingService...")
        row_records = grpc_fetcher.poll_events(
            schema_name="tpch",
            table_name="customer",
            buckets=[0, 1, 2],
        )
        
        # Extract binary data from row records
        binary_data_list = RowRecordBinaryExtractor.extract_records_binary(row_records)
        
        # Parse binary data
        logger.info(f"Parsing {len(binary_data_list)} records...")
        parsed_records = parser.parse_batch(binary_data_list)
        
        # Store in LanceDB with upsert
        logger.info(f"Upserting {len(parsed_records)} records to LanceDB...")
        store.upsert(
            records=parsed_records,
            table_name="customer",
            pk=["custID"],
        )
        
        logger.info("Successfully completed gRPC polling and storage!")
        
    except Exception as e:
        logger.error(f"Error during gRPC polling: {e}")
        raise
    finally:
        grpc_fetcher.close()
        store.close()


def example_grpc_polling_multiple_tables():
    """Example: Poll multiple tables from PixelsPollingService"""
    
    config = ConfigManager("config/config.yaml").get()
    
    # Tables to poll
    tables_to_poll = [
        ("customer", ["custID"]),
        ("company", ["companyID"]),
        ("savingAccount", ["accountID"]),
    ]
    
    grpc_fetcher = PixelsGrpcFetcher(
        host=config.rpc.grpc_host or "localhost",
        port=config.rpc.grpc_port,
    )
    
    store = LanceDBStore(
        db_path=config.lancedb.db_path,
        mode=config.lancedb.mode,
    )
    
    try:
        grpc_fetcher.connect()
        
        for table_name, pk_columns in tables_to_poll:
            logger.info(f"Processing table: {table_name}")
            
            # Initialize parser for this table
            parser = DataParser(
                schema_path="config/schema_hybench.yaml",
                table_name=table_name
            )
            
            # Poll events
            row_records = grpc_fetcher.poll_events(
                schema_name="tpch",
                table_name=table_name,
            )
            
            # Extract and parse
            binary_data_list = RowRecordBinaryExtractor.extract_records_binary(row_records)
            parsed_records = parser.parse_batch(binary_data_list)
            
            # Upsert
            if parsed_records:
                store.upsert(
                    records=parsed_records,
                    table_name=table_name,
                    pk=pk_columns,
                )
                logger.info(f"Upserted {len(parsed_records)} records to {table_name}")
            
    finally:
        grpc_fetcher.close()
        store.close()


if __name__ == "__main__":
    # Uncomment to run examples (requires running PixelsPollingService)
    # example_grpc_polling()
    # example_grpc_polling_multiple_tables()
    
    print("gRPC examples defined. See docstrings for usage.")
    print("Note: These examples require a running PixelsPollingService on localhost:6688")
