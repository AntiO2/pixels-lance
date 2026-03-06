#!/usr/bin/env python3
"""
Parallel fetch script for multiple schema types (HyBench, TPC-CH, etc.) using ThreadPoolExecutor.
Usage: python3 scripts/fetch_all_tables.py [--schema-type hybench|chbenchmark] [--output-mode print|store] [--bucket-num 4] [--workers 8]
"""

import argparse
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

# Schema configurations
SCHEMA_CONFIGS = {
    "hybench": {
        "rpc_schema": "pixels_bench",
        "schema_file": "config/schema_hybench.yaml",
        "tables": [
            "customer",
            "company",
            "savingaccount",
            "checkingaccount",
            "transfer",
            "checking",
            "loanapps",
            "loantrans",
        ],
    },
    "chbenchmark": {
        "rpc_schema": "pixels_bench",  # TPC-CH uses pixels_bench schema
        "schema_file": "config/schema_chbenchmark.yaml",
        "tables": [
            "warehouse",
            "district",
            "customer",
            "history",
            "neworder",
            "order",
            "orderline",
            "item",
            "stock",
            "nation",
            "supplier",
            "region",
        ],
    },
    "tpch": {  # Alias for chbenchmark
        "rpc_schema": "pixels_bench",  # TPC-CH uses pixels_bench schema
        "schema_file": "config/schema_chbenchmark.yaml",
        "tables": [
            "warehouse",
            "district",
            "customer",
            "history",
            "neworder",
            "order",
            "orderline",
            "item",
            "stock",
            "nation",
            "supplier",
            "region",
        ],
    },
}


def fetch_table_bucket(
    table_name: str,
    schema: str,
    schema_file: str,
    output_mode: str,
    bucket_id: int,
    project_root: Path,
) -> Tuple[str, int, bool, str]:
    """
    Fetch a single table for a specific bucket using the CLI.
    Returns: (table_name, bucket_id, success, message)
    """
    try:
        cli_path = project_root / "src" / "pixels_lance" / "cli.py"
        config_path = project_root / "config" / "config.yaml"
        schema_path = project_root / schema_file
        
        cmd = [
            sys.executable,
            str(cli_path),
            "--config",
            str(config_path),
            "--schema-file",
            str(schema_path),
            "--schema",
            schema,
            "--table",
            table_name,
            "--bucket-id",
            str(bucket_id),
            "--output",
            output_mode,
        ]

        result = subprocess.run(
            cmd,
            cwd=str(project_root),
            capture_output=True,
            text=True,
            timeout=300,  # 5 minute timeout per table
        )

        if result.returncode == 0:
            # Extract record count from output if available
            lines = result.stderr.split("\n")
            count_msg = ""
            for line in reversed(lines):
                if "Successfully stored" in line or "records" in line:
                    count_msg = f" ({line.strip()})"
                    break
            return (table_name, bucket_id, True, f"Success{count_msg}")
        else:
            error_msg = result.stderr.split("\n")[-2] if result.stderr else "Unknown error"
            return (table_name, bucket_id, False, f"Failed: {error_msg[:50]}")
    except subprocess.TimeoutExpired:
        return (table_name, bucket_id, False, "Timeout (>5min)")
    except Exception as e:
        return (table_name, bucket_id, False, str(e)[:50])


def fetch_table_all_buckets(
    table_name: str,
    schema: str,
    schema_file: str,
    output_mode: str,
    bucket_num: int,
    project_root: Path,
) -> List[Tuple[str, int, bool, str]]:
    """Fetch all buckets for one table sequentially to avoid same-table write conflicts."""
    results = []
    for bucket_id in range(bucket_num):
        results.append(
            fetch_table_bucket(table_name, schema, schema_file, output_mode, bucket_id, project_root)
        )
    return results


def main():
    parser = argparse.ArgumentParser(description="Parallel fetch for multiple schema types")
    parser.add_argument(
        "--schema-type",
        choices=list(SCHEMA_CONFIGS.keys()),
        default="hybench",
        help="Schema type (default: hybench)",
    )
    parser.add_argument(
        "--output-mode",
        choices=["print", "store"],
        default="store",
        help="Output mode (default: store)",
    )
    parser.add_argument(
        "--bucket-num", type=int, default=4, help="Number of buckets to fetch (default: 4)"
    )
    parser.add_argument(
        "--workers", type=int, default=8, help="Number of parallel workers (default: 8)"
    )
    parser.add_argument(
        "--tables", nargs="+", help="Specific tables to fetch (default: all for schema type)"
    )

    args = parser.parse_args()

    # Get config for selected schema type
    if args.schema_type not in SCHEMA_CONFIGS:
        print(f"Error: Unknown schema type '{args.schema_type}'")
        print(f"Supported types: {', '.join(SCHEMA_CONFIGS.keys())}")
        sys.exit(1)

    config = SCHEMA_CONFIGS[args.schema_type]
    rpc_schema = config["rpc_schema"]
    schema_file = config["schema_file"]
    tables = args.tables if args.tables else config["tables"]

    # Resolve project root from script location: <repo>/scripts/fetch_all_tables.py
    project_root = Path(__file__).resolve().parent.parent

    # Table-level tasks (each table handles its buckets sequentially)
    table_tasks = list(tables)
    total_tasks = len(tables) * args.bucket_num

    print("=" * 70)
    print("Parallel Multi-Schema Table Fetch")
    print(f"Schema Type: {args.schema_type}")
    print(f"RPC Schema: {rpc_schema}")
    print(f"Schema File: {schema_file}")
    print(f"Output Mode: {args.output_mode}")
    print(f"Buckets: {args.bucket_num} (0-{args.bucket_num - 1})")
    print(f"Tables: {len(tables)}")
    print(f"Total Tasks: {total_tasks} (tables × buckets)")
    print(f"Max Workers: {args.workers}")
    print(f"Project Root: {project_root}")
    print("Concurrency Strategy: tables parallel, buckets per table sequential")
    print("=" * 70)

    start_time = datetime.now()
    successful = 0
    failed = 0
    results = []

    # Use ThreadPoolExecutor for parallel fetching
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(
                fetch_table_all_buckets,
                table,
                rpc_schema,
                schema_file,
                args.output_mode,
                args.bucket_num,
                project_root,
            ): table
            for table in table_tasks
        }

        for future in as_completed(futures):
            table_results = future.result()
            for table_name, bucket_id, success, message in table_results:
                status = "✓" if success else "✗"
                task_label = f"{table_name}[B{bucket_id}]"
                results.append((table_name, bucket_id, success, message))
                print(f"[{task_label:<25}] {status} {message}")

                if success:
                    successful += 1
                else:
                    failed += 1

    elapsed = (datetime.now() - start_time).total_seconds()

    print("=" * 70)
    print("Results Summary")
    print(f"Total Time: {elapsed:.1f}s")
    print(f"Successful: {successful}/{total_tasks}")
    print(f"Failed: {failed}/{total_tasks}")

    if failed > 0:
        print("\nFailed Tasks:")
        for table_name, bucket_id, success, message in results:
            if not success:
                print(f"  - {table_name}[B{bucket_id}]: {message}")

    print("=" * 70)

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
