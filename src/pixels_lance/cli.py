"""
Command-line interface for Pixels Lance
"""

import argparse
import sys
from pathlib import Path
from typing import List, Optional

from .config import ConfigManager
from .grpc_fetcher import PixelsGrpcFetcher
from .fetcher import RowRecordBinaryExtractor
from .logger import setup_logging
from .parser import DataParser
from .storage import LanceDBStore


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
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run without saving to LanceDB",
    )

    args = parser.parse_args()

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
        print(f"Dry run: {args.dry_run}")

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

        if not args.dry_run:
            store = LanceDBStore(config=config.lancedb)
            store.create_table(table_name=args.table)

        # Poll events from PixelsPollingService
        print(f"Polling events for table '{args.table}' from schema '{args.schema}'...")
        row_records = grpc_fetcher.poll_events(
            schema_name=args.schema,
            table_name=args.table,
            buckets=args.bucket_id,
        )

        if not row_records:
            print("No records received from gRPC service")
            return 0

        print(f"Received {len(row_records)} row records")

        # Extract binary data from row records
        binary_data_list = RowRecordBinaryExtractor.extract_records_binary(row_records)
        print(f"Extracted {len(binary_data_list)} binary records")

        if not binary_data_list:
            print("No valid binary data extracted")
            return 0

        # Parse binary data
        print("Parsing binary data...")
        parsed_records = parser.parse_batch(binary_data_list)
        print(f"Parsed {len(parsed_records)} records")

        if not args.dry_run:
            # Store in LanceDB with upsert
            print(f"Upserting {len(parsed_records)} records to LanceDB table '{args.table}'...")
            store.upsert(
                records=parsed_records,
                table_name=args.table,
                pk=parser.schema.pk,
            )
            print(f"Successfully stored {len(parsed_records)} records")
        else:
            print(f"Dry run: Would have stored {len(parsed_records)} records")
            # Print first record as example
            if parsed_records:
                print("Sample record:")
                for key, value in parsed_records[0].items():
                    print(f"  {key}: {value}")

        return 0

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
