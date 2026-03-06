#!/usr/bin/env python3
"""
Parallel fetch script for all HyBench tables using ThreadPoolExecutor.
Usage: python3 fetch_all_tables.py [--output-mode print|store] [--bucket-num 4] [--workers 8]
"""

import argparse
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Tuple

# Configuration
SCHEMA = "pixels_bench"
TABLES = [
    "customer",
    "company", 
    "savingAccount",
    "checkingAccount",
    "transfer",
    "checking",
    "loanapps",
    "loantrans"
]

def fetch_table(table_name: str, schema: str, output_mode: str, bucket_id: int, project_root: Path) -> Tuple[str, int, bool, str]:
    """
    Fetch a single table for a specific bucket using the CLI.
    Returns: (table_name, bucket_id, success, message)
    """
    try:
        cmd = [
            sys.executable, "src/pixels_lance/cli.py",
            "--schema", schema,
            "--table", table_name,
            "--bucket-id", str(bucket_id),
            "--output", output_mode
        ]
        
        result = subprocess.run(
            cmd,
            cwd=str(project_root),
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout per table
        )
        
        if result.returncode == 0:
            # Extract record count from output if available
            lines = result.stderr.split('\n')
            count_msg = ""
            for line in reversed(lines):
                if "Successfully stored" in line or "records" in line:
                    count_msg = f" ({line.strip()})"
                    break
            return (table_name, bucket_id, True, f"Success{count_msg}")
        else:
            error_msg = result.stderr.split('\n')[-2] if result.stderr else "Unknown error"
            return (table_name, bucket_id, False, f"Failed: {error_msg[:50]}")
    except subprocess.TimeoutExpired:
        return (table_name, bucket_id, False, "Timeout (>5min)")
    except Exception as e:
        return (table_name, bucket_id, False, str(e)[:50])

def main():
    parser = argparse.ArgumentParser(
        description="Parallel fetch for all HyBench tables"
    )
    parser.add_argument("--output-mode", choices=["print", "store"], default="store",
                       help="Output mode (default: store)")
    parser.add_argument("--bucket-num", type=int, default=4,
                       help="Number of buckets to fetch (default: 4)")
    parser.add_argument("--workers", type=int, default=8,
                       help="Number of parallel workers (default: 8)")
    parser.add_argument("--schema", default="pixels_bench",
                       help="Schema name (default: pixels_bench)")
    parser.add_argument("--tables", nargs="+", default=TABLES,
                       help="Specific tables to fetch (default: all)")
    
    args = parser.parse_args()
    
    # Find project root (where config/ exists)
    project_root = Path(__file__).parent
    
    # Generate all (table, bucket) combinations
    tasks = [(table, bucket_id) for table in args.tables for bucket_id in range(args.bucket_num)]
    
    print("=" * 70)
    print(f"Parallel HyBench Table Fetch")
    print(f"Schema: {args.schema}")
    print(f"Output Mode: {args.output_mode}")
    print(f"Buckets: {args.bucket_num} (0-{args.bucket_num-1})")
    print(f"Tables: {len(args.tables)}")
    print(f"Total Tasks: {len(tasks)} (tables × buckets)")
    print(f"Max Workers: {args.workers}")
    print(f"Project Root: {project_root}")
    print("=" * 70)
    
    start_time = datetime.now()
    successful = 0
    failed = 0
    results = []
    
    # Use ThreadPoolExecutor for parallel fetching
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(fetch_table, table, args.schema, args.output_mode, bucket_id, project_root): (table, bucket_id)
            for table, bucket_id in tasks
        }
        
        for future in as_completed(futures):
            table_name, bucket_id, success, message = future.result()
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
    print(f"Results Summary")
    print(f"Total Time: {elapsed:.1f}s")
    print(f"Successful: {successful}/{len(tasks)}")
    print(f"Failed: {failed}/{len(tasks)}")
    
    if failed > 0:
        print("\nFailed Tasks:")
        for table_name, bucket_id, success, message in results:
            if not success:
                print(f"  - {table_name}[B{bucket_id}]: {message}")
    
    print("=" * 70)
    
    sys.exit(0 if failed == 0 else 1)

if __name__ == "__main__":
    main()
