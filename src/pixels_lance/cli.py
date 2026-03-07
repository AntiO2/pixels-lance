"""
Command-line interface for Pixels Lance
"""

import argparse
import sys
import time
import threading
import queue
from pathlib import Path
from typing import List, Optional
from concurrent.futures import ThreadPoolExecutor

# Handle imports - use absolute imports for direct execution
sys.path.insert(0, str(Path(__file__).parent))
from config import ConfigManager
from grpc_fetcher import PixelsGrpcFetcher
from fetcher import RowRecordBinaryExtractor
from logger import setup_logging, get_logger
from parser import DataParser
from storage import LanceDBStore
import proto.sink_pb2 as sink_pb2

logger = get_logger(__name__)


class BackpressureController:
    """Control polling backpressure to prevent memory overflow"""
    
    def __init__(self, max_pending: int):
        """
        Args:
            max_pending: Maximum pending records (buffered + flushing)
        """
        self.max_pending = max_pending
        self.flushing_count = 0
        self.lock = threading.Lock()
    
    def can_accept(self, buffered_count: int) -> bool:
        """Check if can accept more records"""
        with self.lock:
            total_pending = self.flushing_count + buffered_count
            return total_pending < self.max_pending
    
    def start_flush(self, count: int):
        """Mark records as flushing"""
        with self.lock:
            self.flushing_count += count
    
    def finish_flush(self, count: int):
        """Mark flush completed"""
        with self.lock:
            self.flushing_count -= count
            if self.flushing_count < 0:
                self.flushing_count = 0
    
    def get_status(self) -> dict:
        """Get current status"""
        with self.lock:
            return {'flushing': self.flushing_count}


def _flush_batch(insert_snapshot_batch, update_batch, delete_batch, parser, store, table_name, output_mode, logger, backpressure=None):
    """
    Flush accumulated batch records to storage in parallel.
    
    Args:
        insert_snapshot_batch: List of INSERT/SNAPSHOT binary column values
        update_batch: List of UPDATE binary column values
        delete_batch: List of DELETE binary column values
        parser: DataParser instance
        store: LanceDBStore instance
        table_name: Target table name
        output_mode: "store" or "print"
        logger: Logger instance
        backpressure: BackpressureController instance
    """
    total_count = len(insert_snapshot_batch) + len(update_batch) + len(delete_batch)
    if backpressure:
        backpressure.start_flush(total_count)
    
    try:
        def process_insert_snapshot():
            if not insert_snapshot_batch:
                return
            print(f"Flushing {len(insert_snapshot_batch)} INSERT/SNAPSHOT records")
            
            try:
                # Parse binary data
                parsed_records = parser.parse_batch(insert_snapshot_batch, op_type="INSERT")
                print(f"Parsed {len(parsed_records)} records")

                if output_mode == "store":
                    # Store in LanceDB with append add
                    print(f"Adding {len(parsed_records)} records to LanceDB table '{table_name}'...")
                    store.add(
                        records=parsed_records,
                        table_name=table_name,
                        schema=parser.schema.to_pyarrow_schema(),
                    )
                    print(f"Successfully added {len(parsed_records)} records")
                else:  # output_mode == "print"
                    print(f"Print mode: Displaying {len(parsed_records)} records")
                    for i, record in enumerate(parsed_records):
                        field_values = [f"{key}={repr(value)}" for key, value in record.items()]
                        print(f"Record {i+1}: {', '.join(field_values)}")
            except Exception as e:
                print(f"Error processing INSERT/SNAPSHOT batch: {e}")
                logger.exception(f"Failed to process batch of {len(insert_snapshot_batch)} records")
        
        def process_update():
            if not update_batch:
                return
            print(f"Flushing {len(update_batch)} UPDATE records")

            try:
                parsed_records = parser.parse_batch(update_batch, op_type="UPDATE")
                print(f"Parsed {len(parsed_records)} update records")

                if output_mode == "store":
                    print(f"Upserting {len(parsed_records)} records to LanceDB table '{table_name}'...")
                    store.upsert(
                        records=parsed_records,
                        table_name=table_name,
                        pk=parser.schema.pk,
                        schema=parser.schema.to_pyarrow_schema(),
                    )
                    print(f"Successfully upserted {len(parsed_records)} records")
                else:  # output_mode == "print"
                    print(f"Print mode: Displaying {len(parsed_records)} update records")
                    for i, record in enumerate(parsed_records):
                        field_values = [f"{key}={repr(value)}" for key, value in record.items()]
                        print(f"Update Record {i+1}: {', '.join(field_values)}")
            except Exception as e:
                print(f"Error processing UPDATE batch: {e}")
                logger.exception(f"Failed to process update batch of {len(update_batch)} records")
        
        def process_delete():
            if not delete_batch:
                return
            print(f"Flushing {len(delete_batch)} DELETE records")
            
            try:
                # Parse binary data for delete records
                delete_parsed_records = parser.parse_batch(delete_batch, op_type="DELETE")
                print(f"Parsed {len(delete_parsed_records)} delete records")

                if output_mode == "store":
                    # Delete from LanceDB
                    print(f"Deleting {len(delete_parsed_records)} records from LanceDB table '{table_name}'...")
                    store.delete(
                        records=delete_parsed_records,
                        table_name=table_name,
                        pk=parser.schema.pk,
                    )
                    print(f"Successfully deleted {len(delete_parsed_records)} records")
                else:  # output_mode == "print"
                    print(f"Print mode: Would delete {len(delete_parsed_records)} records")
                    for i, record in enumerate(delete_parsed_records):
                        field_types = [f"{key}={type(value).__name__}" for key, value in record.items()]
                        print(f"Delete Record {i+1}: {', '.join(field_types)}")
            except Exception as e:
                print(f"Error processing DELETE batch: {e}")
                logger.exception(f"Failed to process delete batch of {len(delete_batch)} records")
        
        # Process all operation types in parallel
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = []
            futures.append(executor.submit(process_insert_snapshot))
            futures.append(executor.submit(process_update))
            futures.append(executor.submit(process_delete))
            
            # Wait for all operations to complete
            for future in futures:
                future.result()
    finally:
        if backpressure:
            backpressure.finish_flush(total_count)


def main() -> int:
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description="Pixels Lance - gRPC Binary Data Fetcher and LanceDB Storage"
    )

    parser.add_argument(
        "--config",
        type=str,
        default="config/config.yaml",
        help="Path to config file",
    )

    parser.add_argument(
        "--schema-file",
        type=str,
        default=None,
        help="Path to schema definition file (default: from config.yaml parser.schema_file)",
    )

    parser.add_argument(
        "--schema",
        type=str,
        required=True,
        help="Database/schema name for gRPC polling",
    )

    parser.add_argument(
        "--table",
        type=str,
        required=True,
        help="Table name to poll and store",
    )

    parser.add_argument(
        "--bucket-id",
        type=int,
        action="append",
        help="Bucket ID(s) to poll (can specify multiple times)",
    )

    parser.add_argument(
        "--output",
        type=str,
        default="store",
        choices=["store", "print"],
        help="Output mode: 'store' to save to LanceDB, 'print' to display on screen (default: store)",
    )

    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run without saving to LanceDB (equivalent to --output print)",
    )

    args = parser.parse_args()

    # Handle output mode: dry-run overrides output setting
    if args.dry_run:
        output_mode = "print"
    else:
        output_mode = args.output

    # Setup logging
    setup_logging(args.log_level)

    try:
        # Load configuration
        config_manager = ConfigManager(args.config)
        config = config_manager.get()
        
        # Use schema_file from config if not specified via CLI
        schema_file = args.schema_file if args.schema_file else config.parser.schema_file

        # Validate gRPC configuration
        if not config.rpc.use_grpc:
            print("Error: gRPC must be enabled in config. Set rpc.use_grpc: true", file=sys.stderr)
            return 1

        print(f"Configuration loaded: {args.config}")
        print(f"Schema file: {schema_file}")
        print(f"Database schema: {args.schema}")
        print(f"Table: {args.table}")
        print(f"Bucket IDs: {args.bucket_id or 'all'}")
        print(f"gRPC host: {config.rpc.grpc_host}:{config.rpc.grpc_port}")
        print(f"Output mode: {output_mode}")

        # Initialize components
        grpc_fetcher = PixelsGrpcFetcher(
            host=config.rpc.grpc_host,
            port=config.rpc.grpc_port,
            timeout=config.rpc.timeout,
        )

        parser = DataParser(
            schema_path=schema_file,
            table_name=args.table
        )

        # Initialize store only if storing to database
        store = None
        if output_mode == "store":
            store = LanceDBStore(config=config.lancedb)
            store.create_table(table_name=args.table)

        # Poll events from PixelsPollingService
        print(f"Polling events for table '{args.table}' from schema '{args.schema}'...")
        grpc_fetcher.connect()
        
        # Continuous polling loop
        import time
        # Get batch parameters from config
        batch_size = config.rpc.batch_size
        batch_timeout = config.rpc.batch_timeout
        max_pending = getattr(config.rpc, 'max_pending_records', batch_size * 2)
        
        # Initialize backpressure controller
        backpressure = BackpressureController(max_pending)
        print(f"Backpressure control: max_pending={max_pending} records")
        
        # Batch buffers for streaming processing
        insert_snapshot_batch = []
        update_batch = []
        delete_batch = []
        last_flush_time = time.time()

        try:
            while True:
                # Check backpressure before polling
                buffered_count = len(insert_snapshot_batch) + len(update_batch) + len(delete_batch)
                if not backpressure.can_accept(buffered_count):
                    status = backpressure.get_status()
                    print(f"Backpressure: pausing poll (buffered={buffered_count}, flushing={status['flushing']}, max={max_pending})")
                    time.sleep(1)
                    continue
                
                row_records = grpc_fetcher.poll_events(
                    schema_name=args.schema,
                    table_name=args.table,
                    buckets=args.bucket_id,
                )

                if not row_records:
                    print("No records received, checking if batch should be flushed...")
                    # Check if batch should be flushed due to timeout
                    if insert_snapshot_batch or update_batch or delete_batch:
                        elapsed = time.time() - last_flush_time
                        if elapsed > batch_timeout:
                            print(f"Batch timeout ({batch_timeout}s) reached, flushing batch...")
                            _flush_batch(
                                insert_snapshot_batch, update_batch, delete_batch,
                                parser, store, args.table, output_mode, logger, backpressure
                            )
                            insert_snapshot_batch.clear()
                            update_batch.clear()
                            delete_batch.clear()
                            last_flush_time = time.time()
                    
                    time.sleep(1)  # Wait 1 second before next poll
                    continue

                print(f"Received {len(row_records)} row records")

                # Extract binary data from row records
                extracted_data = RowRecordBinaryExtractor.extract_records_binary(row_records)
                print(f"Extracted {len(extracted_data)} binary records with operations")

                if not extracted_data:
                    print("No valid binary data extracted, waiting for next poll...")
                    time.sleep(1)
                    continue

                # Group records by operation type and add to batch buffers
                for op_type, column_values in extracted_data:
                    if op_type in (sink_pb2.OperationType.INSERT, sink_pb2.OperationType.SNAPSHOT):
                        insert_snapshot_batch.append(column_values)
                    elif op_type == sink_pb2.OperationType.UPDATE:
                        update_batch.append(column_values)
                    elif op_type == sink_pb2.OperationType.DELETE:
                        delete_batch.append(column_values)

                # Check if batch size reached
                total_batch_size = len(insert_snapshot_batch) + len(update_batch) + len(delete_batch)
                if total_batch_size >= batch_size:
                    print(f"Batch size ({batch_size}) reached ({total_batch_size} records), flushing...")
                    _flush_batch(
                        insert_snapshot_batch, update_batch, delete_batch,
                        parser, store, args.table, output_mode, logger, backpressure
                    )
                    insert_snapshot_batch.clear()
                    update_batch.clear()
                    delete_batch.clear()
                    last_flush_time = time.time()
                

                # Wait before next poll
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nPolling stopped by user")
            return 0

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
