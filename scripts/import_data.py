#!/usr/bin/env python3
"""
Universal Data Importer for Pixels Lance

Supports multiple data formats and benchmark types:
- TPC-CH .tbl files (pipe-delimited, single directory)
- Single CSV files (comma-delimited, single directory)
- Partitioned CSV files (multiple subdirectories with _part_*.csv)

Automatically detects data format and imports into LanceDB with proper schema enforcement.
"""

import sys
import csv
import gc
from pathlib import Path
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple
import logging
from enum import Enum
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing
import warnings

# Suppress Lance fork-safe warning (we use spawn context)
warnings.filterwarnings("ignore", message="lance is not fork-safe")

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from pixels_lance.parser import DataParser, Schema, SchemaCollection
from pixels_lance.storage import LanceDBStore
from pixels_lance.config import ConfigManager, LanceDBConfig


# Module-level functions for multiprocessing (must be pickleable)
def _stream_parse_and_add_worker(
    file_paths: List[str],
    schema_dict: Dict[str, Any],
    delimiter: str,
    table_name: str,
    lancedb_config_dict: Dict[str, Any],
    batch_limit: int,
    write_lock: Optional[Any] = None,
) -> int:
    """
    Worker: read a list of files and add to Lance in streaming batches.

    Returns:
        Number of records successfully submitted for add.
    """
    # 输出worker初始化信息
    logger.info(
        f"Worker initialized: {len(file_paths)} files to process for table '{table_name}'"
    )
    if file_paths:
        # 显示前3个文件名
        files_to_show = file_paths[:3]
        for i, fpath in enumerate(files_to_show, 1):
            logger.info(f"  File {i}: {Path(fpath).name}")
        if len(file_paths) > 3:
            logger.info(f"  ... and {len(file_paths) - 3} more files")

    schema = Schema.from_dict(schema_dict)
    schema_arrow = schema.to_pyarrow_schema()

    store = LanceDBStore(config=LanceDBConfig(**lancedb_config_dict))

    total = 0
    batch: List[Dict[str, Any]] = []

    def flush_batch() -> int:
        nonlocal batch
        if not batch:
            return 0

        try:
            if write_lock is not None:
                with write_lock:
                    store.add(
                        records=batch,
                        table_name=table_name,
                        schema=schema_arrow,
                    )
            else:
                store.add(
                    records=batch,
                    table_name=table_name,
                    schema=schema_arrow,
                )

            flushed = len(batch)
        finally:
            # 显式清理：创建新的空列表，让旧列表被垃圾回收
            batch = []
            # 触发垃圾回收以立即释放内存（避免内存持续增长）
            gc.collect()
        
        return flushed

    try:
        for file_path in file_paths:
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f, delimiter=delimiter)
                for row in reader:
                    if len(row) < len(schema.fields):
                        continue

                    record: Dict[str, Any] = {}
                    for field_idx, field in enumerate(schema.fields):
                        raw_value = row[field_idx] if field_idx < len(row) else ""
                        record[field.name] = _parse_field_value(raw_value, field.type)

                    batch.append(record)

                    if len(batch) >= batch_limit:
                        total += flush_batch()

        total += flush_batch()
        return total
    except Exception as e:
        logger.error(f"Worker failed for files {file_paths[:2]}...: {e}")
        return total


def _parse_field_value(value: str, field_type: str) -> Any:
    """Parse a field value based on its type"""
    if value == "" or value is None:
        return None
    
    value = value.strip() if isinstance(value, str) else value
    if isinstance(value, str) and not value:
        return None
    
    try:
        if field_type in ("int8", "int16", "int32", "int64"):
            return int(value)
        elif field_type in ("uint8", "uint16", "uint32", "uint64"):
            return int(value)
        elif field_type in ("float32", "float64"):
            return float(value)
        elif field_type == "boolean":
            return bool(int(value))
        elif field_type == "date":
            if isinstance(value, str) and value.strip():
                formats = [
                    "%Y-%m-%d %H:%M:%S.%f",
                    "%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%d",
                ]
                for fmt in formats:
                    try:
                        return datetime.strptime(value, fmt).date()
                    except ValueError:
                        continue
            return None
        elif field_type == "timestamp":
            if isinstance(value, str) and value.strip():
                formats = [
                    "%Y-%m-%d %H:%M:%S.%f",
                    "%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%d",
                ]
                for fmt in formats:
                    try:
                        return datetime.strptime(value, fmt)
                    except ValueError:
                        continue
            return None
        elif field_type in ("varchar", "char", "string"):
            return value
        elif field_type in ("bytes", "binary", "varbinary"):
            return value
        else:
            return value
    except Exception as e:
        logger.debug(f"Failed to parse {field_type} value '{value}': {e}")
        return None


def _schema_to_dict(schema: Schema) -> Dict[str, Any]:
    """Serialize Schema object for multiprocessing transport."""
    return {
        "table_name": schema.table_name,
        "pk": list(schema.pk) if schema.pk else [],
        "fields": [
            {
                "name": f.name,
                "type": f.type,
                "size": f.size,
                "offset": f.offset,
                "precision": f.precision,
                "scale": f.scale,
                "charset": f.charset,
                "nullable": f.nullable,
            }
            for f in schema.fields
        ],
    }


class DataFormat(Enum):
    """Supported data formats"""
    TBL = "tbl"                    # TPC-CH .tbl files (pipe-delimited)
    TBL_PARTITIONED = "tbl_partitioned"  # Partitioned .tbl files
    CSV = "csv"                    # CSV files (comma-delimited)
    CSV_PARTITIONED = "csv_partitioned"  # Partitioned CSV files


class DataImporter:
    """Universal importer for different data formats"""
    
    def __init__(self, schema_path: str, data_source: str, batch_limit: int = 1000000):
        """
        Initialize importer
        
        Args:
            schema_path: Path to schema YAML file
            data_source: Path to data directory or file
            batch_limit: Max records per process batch before flushing to Lance
        """
        self.schema_path = schema_path
        self.data_source = Path(data_source)
        
        # Load config for case sensitivity option
        cm = ConfigManager()
        self.case_sensitive = cm.get().parser.case_sensitive
        
        # Load schema
        schema_obj = Schema.from_yaml(schema_path)
        if isinstance(schema_obj, SchemaCollection):
            self.schemas = schema_obj.schemas
        else:
            self.schemas = {schema_obj.table_name: schema_obj}
        
        # Convert schema keys to lowercase if case_sensitive is False
        if not self.case_sensitive:
            self.schemas = {k.lower(): v for k, v in self.schemas.items()}
        
        # Initialize store
        self.store = LanceDBStore(config=cm.get().lancedb)
        if hasattr(cm.get().lancedb, "model_dump"):
            self.lancedb_config_dict = cm.get().lancedb.model_dump()
        else:
            self.lancedb_config_dict = cm.get().lancedb.dict()
        self.batch_limit = max(1, int(batch_limit))
        
        # Detect data format
        self.data_format = self._detect_format()
        
        logger.info(f"Loaded {len(self.schemas)} table schemas")
        logger.info(f"Data source: {self.data_source}")
        logger.info(f"Detected format: {self.data_format.value}")
    
    def _detect_format(self) -> DataFormat:
        """Detect data format from source based on file extensions"""
        if not self.data_source.exists():
            raise FileNotFoundError(f"Data source not found: {self.data_source}")
        
        if not self.data_source.is_dir():
            raise ValueError(f"Data source must be a directory: {self.data_source}")
        
        # List contents
        contents = list(self.data_source.iterdir())
        if not contents:
            raise ValueError(f"Empty directory: {self.data_source}")
        
        # Separate files and directories
        files = [c for c in contents if c.is_file()]
        subdirs = [c for c in contents if c.is_dir()]
        
        # Check file extensions
        tbl_files = [f for f in files if f.suffix.lower() == ".tbl"]
        csv_files = [f for f in files if f.suffix.lower() == ".csv"]
        
        # Determine format based on file types
        if tbl_files:
            # Has .tbl files - TBL format
            return DataFormat.TBL
        elif csv_files:
            # Has .csv files in root - check if also has subdirectories with CSV
            csv_in_subdirs = any(
                list(d.glob("*.csv")) for d in subdirs if d.is_dir()
            )
            if csv_in_subdirs:
                # Mixed structure - shouldn't happen, prefer root CSVs
                logger.warning("Found CSV files both in root and subdirectories")
            return DataFormat.CSV
        elif subdirs:
            # No files in root, check subdirectories
            # Check what file type is in subdirectories
            for subdir in subdirs:
                subdir_files = list(subdir.glob("*.tbl"))
                if subdir_files:
                    return DataFormat.TBL_PARTITIONED
                
                subdir_files = list(subdir.glob("*.csv"))
                if subdir_files:
                    return DataFormat.CSV_PARTITIONED
            
            raise ValueError(f"No supported file types found in {self.data_source}")
        else:
            raise ValueError(f"No data files found in {self.data_source}")
    
    def _parse_field_value(self, value: str, field_type: str) -> Any:
        """Parse a field value based on its type (delegate to module-level function)"""
        return _parse_field_value(value, field_type)
    
    def _parse_csv_file(self, csv_file: Path, schema: Schema, delimiter: str = ",") -> List[Dict[str, Any]]:
        """Parse a CSV file"""
        records = []
        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.reader(f, delimiter=delimiter)
                for row_num, row in enumerate(reader, 1):
                    if len(row) < len(schema.fields):
                        continue
                    
                    record = {}
                    for field_idx, field in enumerate(schema.fields):
                        raw_value = row[field_idx] if field_idx < len(row) else ""
                        parsed_value = self._parse_field_value(raw_value, field.type)
                        record[field.name] = parsed_value
                    
                    records.append(record)
        except Exception as e:
            logger.warning(f"Error reading {csv_file.name}: {e}")
            return []
        
        return records

    def _split_files_for_workers(self, files: List[Path], max_workers: int) -> List[List[str]]:
        """Split files evenly by workers and return groups of file paths."""
        if not files:
            return []

        worker_count = 1 if len(files) == 1 else min(max_workers, len(files))
        groups: List[List[str]] = [[] for _ in range(worker_count)]

        for idx, file_path in enumerate(files):
            groups[idx % worker_count].append(str(file_path))

        return [g for g in groups if g]
    
    def _import_tbl_format(self, table_name: str, max_workers: int = 4) -> int:
        """Import TBL format (pipe-delimited, with or without partitions)"""
        # Normalize table name based on case_sensitive setting
        lookup_name = table_name if self.case_sensitive else table_name.lower()
        schema = self.schemas.get(lookup_name)
        if not schema:
            logger.error(f"No schema for '{table_name}'")
            return 0
        
        files_to_import = []
        
        # Check if partitioned (in subdirectory) or single file
        if self.data_format == DataFormat.TBL_PARTITIONED:
            # Find table subdirectory
            table_dir = None
            for d in self.data_source.iterdir():
                if d.is_dir() and d.name.lower() == table_name.lower():
                    table_dir = d
                    break
            
            if not table_dir:
                logger.error(f"Table directory not found for '{table_name}'")
                return 0
            
            files_to_import = sorted(table_dir.glob("*.tbl"))
            if not files_to_import:
                logger.error(f"No TBL files found in {table_dir}")
                return 0
            
            logger.info(f"Importing {table_name} from {len(files_to_import)} TBL partitions...")
        else:
            # Single file in root
            for f in self.data_source.glob("*.tbl"):
                if f.stem.lower() == table_name.lower():
                    files_to_import = [f]
                    break
            
            if not files_to_import:
                logger.error(f"TBL file not found for '{table_name}'")
                return 0
            
            logger.info(f"Importing {table_name} from {files_to_import[0].name} (pipe-delimited)...")
        
        # Process by worker file-groups: each process receives a files list
        schema_dict = _schema_to_dict(schema)
        file_groups = self._split_files_for_workers(files_to_import, max_workers)
        worker_count = len(file_groups)
        
        # Always convert table name to lowercase for Lance dataset storage
        dataset_name = table_name.lower()

        logger.info(
            f"Starting {worker_count} worker processes (batch_limit={self.batch_limit:,})..."
        )

        manager = multiprocessing.Manager()
        write_lock = manager.Lock()
        total_written = 0

        # Convert table name to lowercase for dataset name
        dataset_name = table_name.lower()

        with ProcessPoolExecutor(
            max_workers=worker_count,
            mp_context=multiprocessing.get_context("spawn"),
        ) as executor:
            futures = {
                executor.submit(
                    _stream_parse_and_add_worker,
                    group,
                    schema_dict,
                    "|",
                    dataset_name,
                    self.lancedb_config_dict,
                    self.batch_limit,
                    write_lock,
                ): i
                for i, group in enumerate(file_groups, 1)
            }

            for future in as_completed(futures):
                worker_id = futures[future]
                try:
                    written = future.result()
                    total_written += written
                    logger.info(f"  Worker {worker_id}/{worker_count}: {written} records added")
                except Exception as e:
                    logger.error(f"Error in worker {worker_id}: {e}")

        manager.shutdown()

        if total_written <= 0:
            logger.warning(f"No records found for table '{table_name}'")
            return 0

        logger.info(f"✓ Imported {total_written} records for '{dataset_name}'")
        return total_written
    
    def _import_csv_format(self, table_name: str, max_workers: int = 4) -> int:
        """Import CSV format (comma-delimited, with or without partitions)"""
        # Normalize table name based on case_sensitive setting
        lookup_name = table_name if self.case_sensitive else table_name.lower()
        schema = self.schemas.get(lookup_name)
        if not schema:
            logger.error(f"No schema for '{table_name}'")
            return 0
        
        files_to_import = []
        
        # Check if partitioned (in subdirectory) or single file
        if self.data_format == DataFormat.CSV_PARTITIONED:
            # Find table subdirectory
            table_dir = None
            for d in self.data_source.iterdir():
                if d.is_dir() and d.name.lower() == table_name.lower():
                    table_dir = d
                    break
            
            if not table_dir:
                logger.error(f"Table directory not found for '{table_name}'")
                return 0
            
            files_to_import = sorted(table_dir.glob("*.csv"))
            if not files_to_import:
                logger.error(f"No CSV files found in {table_dir}")
                return 0
            
            logger.info(f"Importing {table_name} from {len(files_to_import)} CSV partitions...")
        else:
            # Single file in root
            for f in self.data_source.glob("*.csv"):
                if f.stem.lower() == table_name.lower():
                    files_to_import = [f]
                    break
            
            if not files_to_import:
                logger.error(f"CSV file not found for '{table_name}'")
                return 0
            
            logger.info(f"Importing {table_name} from {files_to_import[0].name} (comma-delimited)...")
        
        # Process by worker file-groups: each process receives a files list
        schema_dict = _schema_to_dict(schema)
        file_groups = self._split_files_for_workers(files_to_import, max_workers)
        worker_count = len(file_groups)

        logger.info(
            f"Starting {worker_count} worker processes (batch_limit={self.batch_limit:,})..."
        )

        manager = multiprocessing.Manager()
        write_lock = manager.Lock()
        total_written = 0

        # Convert table name to lowercase for dataset name
        dataset_name = table_name.lower()

        with ProcessPoolExecutor(
            max_workers=worker_count,
            mp_context=multiprocessing.get_context("spawn"),
        ) as executor:
            futures = {
                executor.submit(
                    _stream_parse_and_add_worker,
                    group,
                    schema_dict,
                    ",",
                    dataset_name,
                    self.lancedb_config_dict,
                    self.batch_limit,
                    write_lock,
                ): i
                for i, group in enumerate(file_groups, 1)
            }

            for future in as_completed(futures):
                worker_id = futures[future]
                try:
                    written = future.result()
                    total_written += written
                    logger.info(f"  Worker {worker_id}/{worker_count}: {written} records added")
                except Exception as e:
                    logger.error(f"Error in worker {worker_id}: {e}")

        manager.shutdown()

        if total_written <= 0:
            logger.warning(f"No records found for table '{table_name}'")
            return 0

        logger.info(f"✓ Imported {total_written} records for '{dataset_name}'")
        return total_written
    
    def import_table(self, table_name: str, max_workers: int = 4) -> int:
        """Import a single table based on detected format"""
        if self.data_format in (DataFormat.TBL, DataFormat.TBL_PARTITIONED):
            return self._import_tbl_format(table_name, max_workers)
        elif self.data_format in (DataFormat.CSV, DataFormat.CSV_PARTITIONED):
            return self._import_csv_format(table_name, max_workers)
        else:
            logger.error(f"Unsupported format: {self.data_format}")
            return 0
    
    def import_all(self, max_workers: int = 4) -> Dict[str, int]:
        """Import all tables"""
        results = {}
        
        # Determine which tables to import
        tables_to_import = []
        
        if self.data_format in (DataFormat.TBL_PARTITIONED, DataFormat.CSV_PARTITIONED):
            # List subdirectories
            tables_to_import = [d.name for d in self.data_source.iterdir() if d.is_dir()]
        else:
            # List files in root
            if self.data_format == DataFormat.TBL:
                files = list(self.data_source.glob("*.tbl"))
            else:  # CSV
                files = list(self.data_source.glob("*.csv"))
            
            tables_to_import = [f.stem for f in files]
        
        logger.info(f"Found {len(tables_to_import)} data sources")
        
        for table_name in sorted(tables_to_import):
            # Skip if no schema
            lookup_name = table_name if self.case_sensitive else table_name.lower()
            if lookup_name not in self.schemas:
                logger.warning(f"No schema for '{table_name}', skipping")
                continue
            
            count = self.import_table(table_name, max_workers=max_workers)
            results[table_name] = count
        
        return results


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Universal data importer for Pixels Lance",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Import TPC-CH .tbl files
  %(prog)s --schema config/schema_chbenchmark.yaml --data ~/disk2/ch1

  # Import single CSV files
  %(prog)s --schema config/schema_hybench.yaml --data ~/disk1/Data_10x

  # Import partitioned CSV files
  %(prog)s --schema config/schema_hybench.yaml --data ~/disk2/Data_pixels_100x

  # Import specific table only
  %(prog)s --schema config/schema_hybench.yaml --data ~/disk2/Data_pixels_100x --table customer
        """
    )
    
    parser.add_argument(
        "--schema",
        type=str,
        required=True,
        help="Path to schema YAML file",
    )
    
    parser.add_argument(
        "--data",
        type=str,
        required=True,
        help="Data source directory",
    )
    
    parser.add_argument(
        "--table",
        type=str,
        help="Import specific table only (default: import all)",
    )
    
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of parallel workers for partitioned data (default: 4)",
    )

    parser.add_argument(
        "--batch-limit",
        type=int,
        default=1000000,
        help="Max records per worker batch before writing to Lance (default: 3000000)",
    )
    
    args = parser.parse_args()
    
    # Validate paths
    schema_path = Path(args.schema)
    data_source = Path(args.data)
    
    if not schema_path.exists():
        logger.error(f"Schema file not found: {schema_path}")
        return 1
    
    if not data_source.exists():
        logger.error(f"Data source not found: {data_source}")
        return 1
    
    # Initialize importer
    try:
        importer = DataImporter(
            str(schema_path),
            str(data_source),
            batch_limit=args.batch_limit,
        )
    except Exception as e:
        logger.error(f"Failed to initialize importer: {e}")
        return 1
    
    # Import
    logger.info("=" * 70)
    logger.info("Starting data import")
    logger.info("=" * 70)
    
    if args.table:
        count = importer.import_table(args.table, max_workers=args.workers)
        logger.info(f"\nImported {count} records for '{args.table}'")
        return 0 if count > 0 else 1
    else:
        results = importer.import_all(max_workers=args.workers)
        
        logger.info("\n" + "=" * 70)
        logger.info("Import Summary")
        logger.info("=" * 70)
        total = 0
        for table_name in sorted(results.keys()):
            count = results[table_name]
            if count > 0:
                logger.info(f"  {table_name:30s}: {count:>12,} records")
                total += count
        
        logger.info("=" * 70)
        logger.info(f"Total imported: {total:,} records")
        logger.info("=" * 70)
        
        return 0


if __name__ == "__main__":
    sys.exit(main())
