#!/usr/bin/env python3
"""
Parallel fetch script for multiple schema types (HyBench, TPC-CH, etc.) using thread/process executors.
Usage: python3 scripts/fetch_all_tables.py [--schema-type hybench|chbenchmark] [--output-mode print|store] [--bucket-num 4] [--execution-mode thread|process]
"""

import argparse
import csv
import subprocess
import sys
import threading
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    print("Warning: psutil not installed. Resource monitoring disabled.")
    print("Install with: pip install psutil")

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


class ResourceMonitor:
    """Monitor CPU and memory usage of child processes"""
    
    def __init__(self, interval: float = 2.0, output_path: Optional[str] = None):
        """
        Args:
            interval: Sampling interval in seconds
            output_path: Path to write real-time monitoring data (CSV format)
        """
        self.interval = interval
        self.output_path = output_path
        self.monitoring = False
        self.monitor_thread = None
        self.stats = {
            'cpu_samples': [],
            'memory_samples': [],
            'process_count_samples': [],
            'memory_mb_samples': [],
            'cpu_count_samples': [],
        }
        self.current_pid = psutil.Process().pid if PSUTIL_AVAILABLE else None
        self.csv_file = None
        self.csv_writer = None
        
        # Initialize CSV file if output path is provided
        if self.output_path and PSUTIL_AVAILABLE:
            output_file = Path(self.output_path)
            output_file.parent.mkdir(parents=True, exist_ok=True)
            self.csv_file = open(output_file, 'w', newline='')
            self.csv_writer = csv.writer(self.csv_file)
            # Write header
            self.csv_writer.writerow(['timestamp', 'cpu_percent', 'memory_percent', 'memory_mb', 'cpu_count', 'process_count'])
            self.csv_file.flush()
        
    def _monitor_loop(self):
        """Background monitoring loop"""
        if not PSUTIL_AVAILABLE:
            return
            
        while self.monitoring:
            try:
                # Get current process
                current = psutil.Process(self.current_pid)
                
                # Find all child processes (including descendants)
                children = current.children(recursive=True)
                
                total_cpu = 0.0
                total_memory = 0.0
                total_memory_mb = 0.0
                process_count = len(children)
                cpu_count = psutil.cpu_count()
                
                for child in children:
                    try:
                        # Get CPU and memory percent for each child
                        cpu_percent = child.cpu_percent(interval=0.1)
                        memory_percent = child.memory_percent()
                        memory_info = child.memory_info()
                        
                        total_cpu += cpu_percent
                        total_memory += memory_percent
                        total_memory_mb += memory_info.rss / (1024 * 1024)  # Convert bytes to MB
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        continue
                
                # Record stats
                self.stats['cpu_samples'].append(total_cpu)
                self.stats['memory_samples'].append(total_memory)
                self.stats['process_count_samples'].append(process_count)
                self.stats['memory_mb_samples'].append(total_memory_mb)
                self.stats['cpu_count_samples'].append(cpu_count)
                
                # Write to CSV file in real-time
                if self.csv_writer:
                    timestamp = datetime.now().isoformat()
                    self.csv_writer.writerow([timestamp, f"{total_cpu:.2f}", f"{total_memory:.2f}", f"{total_memory_mb:.2f}", cpu_count, process_count])
                    self.csv_file.flush()
                
            except Exception as e:
                print(f"Warning: Resource monitoring error: {e}")
            
            time.sleep(self.interval)
    
    def start(self):
        """Start monitoring"""
        if not PSUTIL_AVAILABLE:
            return
            
        self.monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        
    def stop(self):
        """Stop monitoring"""
        if not PSUTIL_AVAILABLE:
            return
            
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5.0)
        
        # Close CSV file
        if self.csv_file:
            self.csv_file.close()
            print(f"Resource monitoring data saved to: {self.output_path}")
    
    def get_stats(self) -> dict:
        """Get statistics summary"""
        if not PSUTIL_AVAILABLE or not self.stats['cpu_samples']:
            return {
                'max_cpu': 0,
                'avg_cpu': 0,
                'max_memory': 0,
                'avg_memory': 0,
                'max_memory_mb': 0,
                'avg_memory_mb': 0,
                'cpu_count': 0,
                'max_processes': 0,
            }
        
        return {
            'max_cpu': max(self.stats['cpu_samples']),
            'avg_cpu': sum(self.stats['cpu_samples']) / len(self.stats['cpu_samples']),
            'max_memory': max(self.stats['memory_samples']),
            'avg_memory': sum(self.stats['memory_samples']) / len(self.stats['memory_samples']),
            'max_memory_mb': max(self.stats['memory_mb_samples']) if self.stats['memory_mb_samples'] else 0,
            'avg_memory_mb': sum(self.stats['memory_mb_samples']) / len(self.stats['memory_mb_samples']) if self.stats['memory_mb_samples'] else 0,
            'cpu_count': self.stats['cpu_count_samples'][0] if self.stats['cpu_count_samples'] else 0,
            'max_processes': max(self.stats['process_count_samples']) if self.stats['process_count_samples'] else 0,
        }


def fetch_table_bucket(
    table_name: str,
    schema: str,
    schema_file: str,
    output_mode: str,
    bucket_id: int,
    project_root: Path,
    timeout: int = 300,
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
            timeout=timeout,
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
        return (table_name, bucket_id, False, f"Timeout (>{timeout}s)")
    except Exception as e:
        return (table_name, bucket_id, False, str(e)[:50])


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
        "--tables", nargs="+", help="Specific tables to fetch (default: all for schema type)"
    )
    parser.add_argument(
        "--execution-mode",
        choices=["thread", "process"],
        default="process",
        help="Parallel execution mode (default: process)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=300,
        help="Timeout in seconds for each task (default: 300)",
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

    # Load monitoring configuration from config.yaml
    monitor_config = {}
    config_path = project_root / "config" / "config.yaml"
    if YAML_AVAILABLE and config_path.exists():
        try:
            with open(config_path) as f:
                full_config = yaml.safe_load(f)
                monitor_config = full_config.get('monitor', {})
        except Exception as e:
            print(f"Warning: Failed to load monitor config: {e}")
    
    monitor_enabled = monitor_config.get('enabled', True)
    monitor_output_path = monitor_config.get('output_path', './monitoring/resource_stats.csv')
    monitor_interval = monitor_config.get('interval', 2.0)

    # Task-level parallelism: every (table, bucket) runs independently
    task_pairs = [(table, bucket_id) for table in tables for bucket_id in range(args.bucket_num)]
    total_tasks = len(task_pairs)
    worker_count = total_tasks

    print("=" * 70)
    print("Parallel Multi-Schema Table Fetch")
    print(f"Schema Type: {args.schema_type}")
    print(f"RPC Schema: {rpc_schema}")
    print(f"Schema File: {schema_file}")
    print(f"Output Mode: {args.output_mode}")
    print(f"Buckets: {args.bucket_num} (0-{args.bucket_num - 1})")
    print(f"Tables: {len(tables)}")
    print(f"Total Tasks: {total_tasks} (tables × buckets)")
    print(f"Workers: {worker_count} (auto, one per task)")
    print(f"Execution Mode: {args.execution_mode}")
    print(f"Project Root: {project_root}")
    print("Concurrency Strategy: tables and buckets fully parallel")
    print("=" * 70)

    start_time = datetime.now()
    successful = 0
    failed = 0
    results = []

    # Start resource monitoring
    monitor_path = str(project_root / monitor_output_path) if monitor_enabled else None
    monitor = ResourceMonitor(interval=monitor_interval, output_path=monitor_path)
    if PSUTIL_AVAILABLE and monitor_enabled:
        print(f"Starting resource monitor (output: {monitor_path})...")
        monitor.start()
        print()

    # Use selected executor for full task-level parallel fetching
    executor_cls = ProcessPoolExecutor if args.execution_mode == "process" else ThreadPoolExecutor
    with executor_cls(max_workers=worker_count) as executor:
        futures = {
            executor.submit(
                fetch_table_bucket,
                table,
                rpc_schema,
                schema_file,
                args.output_mode,
                bucket_id,
                project_root,
                args.timeout,
            ): (table, bucket_id)
            for table, bucket_id in task_pairs
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

    # Stop resource monitoring
    if PSUTIL_AVAILABLE:
        monitor.stop()

    elapsed = (datetime.now() - start_time).total_seconds()

    # Get resource statistics
    resource_stats = monitor.get_stats()

    print("=" * 70)
    print("Results Summary")
    print(f"Total Time: {elapsed:.1f}s")
    print(f"Successful: {successful}/{total_tasks}")
    print(f"Failed: {failed}/{total_tasks}")

    if PSUTIL_AVAILABLE and resource_stats['max_cpu'] > 0:
        print()
        print("Resource Usage Statistics:")
        print(f"  CPU Cores: {resource_stats['cpu_count']}")
        print(f"  Max Concurrent Processes: {resource_stats['max_processes']}")
        print(f"  CPU Usage: Avg {resource_stats['avg_cpu']:.2f}%, Peak {resource_stats['max_cpu']:.2f}%")
        print(f"  Memory Usage (Percent): Avg {resource_stats['avg_memory']:.2f}%, Peak {resource_stats['max_memory']:.2f}%")
        print(f"  Memory Usage (Total): Avg {resource_stats['avg_memory_mb']:.2f}MB, Peak {resource_stats['max_memory_mb']:.2f}MB")

    if failed > 0:
        print("\nFailed Tasks:")
        for table_name, bucket_id, success, message in results:
            if not success:
                print(f"  - {table_name}[B{bucket_id}]: {message}")

    print("=" * 70)

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
