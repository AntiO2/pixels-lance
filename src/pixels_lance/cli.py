"""
Command-line interface for Pixels Lance
"""

import argparse
import sys
import threading
import time
from pathlib import Path
from typing import List

from .config import ConfigManager
from .fetcher import RpcFetcher
from .logger import setup_logging
from .parser import DataParser, Schema, SchemaCollection
from .storage import LanceDBStore


def main() -> int:
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description="RPC Binary Data Fetcher and LanceDB Storage"
    )

    parser.add_argument(
        "--config",
        type=str,
        default="config/config.yaml",
        help="Path to config file",
    )

    parser.add_argument(
        "--schema",
        type=str,
        default="config/schema.yaml",
        help="Path to schema file",
    )

    parser.add_argument(
        "--table",
        type=str,
        default=None,
        help="Table name within a multi-table schema file",
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

        # set up LanceDB store globally
        store = LanceDBStore(config=config.lancedb)

        # Determine schema type
        schema_obj = Schema.from_yaml(args.schema)
        table_names: List[str]
        if isinstance(schema_obj, SchemaCollection):
            # multiple tables in one file
            table_names = list(schema_obj.schemas.keys())
            if args.table:
                if args.table not in table_names:
                    raise ValueError(f"Table {args.table} not found in schema file")
                table_names = [args.table]
        else:
            table_names = [schema_obj.table_name or args.table]

        print(f"Configuration loaded: {args.config}")
        print(f"Schema file: {args.schema}")
        print(f"Tables to process: {table_names}")
        print(f"Batch size: {config.batch_size}")

        def worker(table_name: str):
            """Thread worker fetching and upserting for a given table"""
            parser_obj = DataParser(schema_path=args.schema, table_name=table_name)
            store.create_table(table_name=table_name)
            fetcher = RpcFetcher(config=config.rpc)

            # placeholder loop; replace with actual RPC logic
            while True:
                # you should call actual rpc method here
                data = fetcher.fetch(f"get_{table_name}")
                if data is None:
                    break
                record = parser_obj.parse(data)
                # perform upsert using primary key from schema
                store.upsert(record, table_name=table_name, key=parser_obj.schema.pk)
                time.sleep(0.1)

        # start threads
        threads = []
        for tbl in table_names:
            t = threading.Thread(target=worker, args=(tbl,))
            t.start()
            threads.append(t)

        # join threads
        for t in threads:
            t.join()

        return 0

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
