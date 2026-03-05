#!/usr/bin/env python3
"""
Parallel fetch script for all HyBench tables using ThreadPoolExecutor.
Usage: python3 fetch_all_tables.py [--output-mode print|store] [--bucket-id 0] [--workers 4]
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

def fetch_table(table_name: str, schema: str, output_mode: str, bucket_id: int, project_root: Path) -> Tuple[str, bool, str]:
    """
    Fetch a single table using the CLI.
    Returns: (table_name, success, message)
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
            return (table_name, True, f"Success{count_msg}")
        else:
            error_msg = result.stderr.split('\n')[-2] if result.stderr else "Unknown error"
            return (table_name, False, f"Failed: {error_msg[:50]}")
    except subprocess.TimeoutExpired:
        return (table_name, False, "Timeout (>5min)")
    except Exception as e:
        return (table_name, False, str(e)[:50])

def main():
    parser = argparse.ArgumentParser(
        description="Parallel fetch for all HyBench tables"
    )
    parser.add_argument("--output-mode", choices=["print", "store"], default="store",
                       help="Output mode (default: store)")
    parser.add_argument("--bucket-id", type=int, default=0,
                       help="Bucket ID (default: 0)")
    parser.add_argument("--workers", type=int, default=4,
                       help="Number of parallel workers (default: 4)")
    parser.add_argument("--schema", default="pixels_bench",
                       help="Schema name (default: pixels_bench)")
    parser.add_argument("--tables", nargs="+", default=TABLES,
                       help="Specific tables to fetch (default: all)")
    
    args = parser.parse_args()
    
    # Find project root (where config/ exists)
    project_root = Path(__file__).parent
    
    print("=" * 70)
    print(f"Parallel HyBench Table Fetch")
    print(f"Schema: {args.schema}")
    print(f"Output Mode: {args.output_mode}")
    print(f"Bucket ID: {args.bucket_id}")
    print(f"Tables: {len(args.tables)}")
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
            executor.submit(fetch_table, table, args.schema, args.output_mode, args.bucket_id, project_root): table 
            for table in args.tables
        }
        
        for future in as_completed(futures):
            table_name, success, message = future.result()
            status = "✓" if success else "✗"
            results.append((table_name, success, message))
            print(f"[{table_name:<20}] {status} {message}")
            
            if success:
                successful += 1
            else:
                failed += 1
    
    elapsed = (datetime.now() - start_time).total_seconds()
    
    print("=" * 70)
    print(f"Results Summary")
    print(f"Total Time: {elapsed:.1f}s")
    print(f"Successful: {successful}/{len(args.tables)}")
    print(f"Failed: {failed}/{len(args.tables)}")
    
    if failed > 0:
        print("\nFailed Tables:")
        for table_name, success, message in results:
            if not success:
                print(f"  - {table_name}: {message}")
    
    print("=" * 70)
    
    sys.exit(0 if failed == 0 else 1)

if __name__ == "__main__":
    main()
