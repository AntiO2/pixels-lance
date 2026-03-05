"""
Command-line interface for Pixels Lance
"""

import argparse
import sys
from pathlib import Path
from typing import List, Optional

# Handle imports - use absolute imports for direct execution
sys.path.insert(0, str(Path(__file__).parent))
from config import ConfigManager
from grpc_fetcher import PixelsGrpcFetcher
from fetcher import RowRecordBinaryExtractor
from logger import setup_logging
from parser import DataParser
from storage import LanceDBStore
import proto.sink_pb2 as sink_pb2


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
        default="config/schema_hybench.yaml",
        help="Path to schema definition file",
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

        # Validate gRPC configuration
        if not config.rpc.use_grpc:
            print("Error: gRPC must be enabled in config. Set rpc.use_grpc: true", file=sys.stderr)
            return 1

        print(f"Configuration loaded: {args.config}")
        print(f"Schema file: {args.schema_file}")
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
            schema_path=args.schema_file,
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
        try:
            while True:
                row_records = grpc_fetcher.poll_events(
                    schema_name=args.schema,
                    table_name=args.table,
                    buckets=args.bucket_id,
                )

                if not row_records:
                    print("No records received, waiting for next poll...")
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

                # Group records by operation type
                insert_update_records = []
                delete_records = []
                
                for op_type, column_values in extracted_data:
                    if op_type in (sink_pb2.OperationType.INSERT, sink_pb2.OperationType.UPDATE, sink_pb2.OperationType.SNAPSHOT):
                        insert_update_records.append(column_values)
                    elif op_type == sink_pb2.OperationType.DELETE:
                        delete_records.append(column_values)

                # Process INSERT/UPDATE/SNAPSHOT records
                if insert_update_records:
                    print(f"Processing {len(insert_update_records)} INSERT/UPDATE/SNAPSHOT records")
                    
                    # Parse binary data
                    print("Parsing binary data...")
                    parsed_records = parser.parse_batch(insert_update_records)
                    print(f"Parsed {len(parsed_records)} records")

                    # Debug: Check first record
                    if insert_update_records and parsed_records:
                        first_columns = insert_update_records[0]
                        print(f"First record has {len(first_columns)} columns")
                        # Show all columns with their byte lengths
                        for i, col_data in enumerate(first_columns):
                            field_name = parser.schema.fields[i].name if i < len(parser.schema.fields) else f"column_{i}"
                            print(f"  Column {i} ({field_name}): {len(col_data)} bytes, hex={col_data.hex()[:40]}...")
                        
                        first_record = parsed_records[0]
                        print(f"First parsed record has {len(first_record)} fields")
                        for key, value in first_record.items():
                            print(f"  {key}: {repr(value)} (type: {type(value).__name__})")

                    if output_mode == "store":
                        # Store in LanceDB with upsert
                        print(f"Upserting {len(parsed_records)} records to LanceDB table '{args.table}'...")
                        store.upsert(
                            records=parsed_records,
                            table_name=args.table,
                            pk=parser.schema.pk,
                        )
                        print(f"Successfully stored {len(parsed_records)} records")
                    else:  # output_mode == "print"
                        print(f"Print mode: Displaying {len(parsed_records)} records")
                        # Print all records (one line per record, showing field values)
                        for i, record in enumerate(parsed_records):
                            field_values = [f"{key}={repr(value)}" for key, value in record.items()]
                            print(f"Record {i+1}: {', '.join(field_values)}")
                # Process DELETE records
                if delete_records:
                    print(f"Processing {len(delete_records)} DELETE records")
                    
                    # Parse binary data for delete records
                    print("Parsing delete records...")
                    delete_parsed_records = parser.parse_batch(delete_records)
                    print(f"Parsed {len(delete_parsed_records)} delete records")

                    if output_mode == "store":
                        # Delete from LanceDB
                        print(f"Deleting {len(delete_parsed_records)} records from LanceDB table '{args.table}'...")
                        store.delete(
                            records=delete_parsed_records,
                            table_name=args.table,
                            key=parser.schema.pk,
                        )
                        print(f"Successfully deleted {len(delete_parsed_records)} records")
                    else:  # output_mode == "print"
                        print(f"Print mode: Would delete {len(delete_parsed_records)} records")
                        # Print delete records
                        for i, record in enumerate(delete_parsed_records):
                            field_types = [f"{key}={type(value).__name__}" for key, value in record.items()]
                            print(f"Delete Record {i+1}: {', '.join(field_types)}")
                
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
