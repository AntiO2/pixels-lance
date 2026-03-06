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
from pathlib import Path
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple
import logging
from enum import Enum
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing

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
from pixels_lance.config import ConfigManager


# Module-level functions for multiprocessing (must be pickleable)
def _parse_csv_file_worker(file_path: str, schema_dict: Dict, delimiter: str) -> List[Dict[str, Any]]:
    """
    Worker function for parsing CSV files in a separate process.
    
    Args:
        file_path: Path to CSV file
        schema_dict: Schema as dict (for pickling)
        delimiter: CSV delimiter
    
    Returns:
        List of parsed records
    """
    # Reconstruct schema from dict
    schema = Schema.from_dict(schema_dict)
    
    records = []
    try:
        csv_file = Path(file_path)
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f, delimiter=delimiter)
            for row_num, row in enumerate(reader, 1):
                if len(row) < len(schema.fields):
                    continue
                
                record = {}
                for field_idx, field in enumerate(schema.fields):
                    raw_value = row[field_idx] if field_idx < len(row) else ""
                    parsed_value = _parse_field_value(raw_value, field.type)
                    record[field.name] = parsed_value
                
                records.append(record)
    except Exception as e:
        logger.warning(f"Error reading {file_path}: {e}")
        return []
    
    return records


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


class DataFormat(Enum):
    """Supported data formats"""
    TBL = "tbl"                    # TPC-CH .tbl files (pipe-delimited)
    TBL_PARTITIONED = "tbl_partitioned"  # Partitioned .tbl files
    CSV = "csv"                    # CSV files (comma-delimited)
    CSV_PARTITIONED = "csv_partitioned"  # Partitioned CSV files


class DataImporter:
    """Universal importer for different data formats"""
    
    def __init__(self, schema_path: str, data_source: str):
        """
        Initialize importer
        
        Args:
            schema_path: Path to schema YAML file
            data_source: Path to data directory or file
        """
        self.schema_path = schema_path
        self.data_source = Path(data_source)
        
        # Load schema
        schema_obj = Schema.from_yaml(schema_path)
        if isinstance(schema_obj, SchemaCollection):
            self.schemas = schema_obj.schemas
        else:
            self.schemas = {schema_obj.table_name: schema_obj}
        
        # Initialize store
        cm = ConfigManager()
        self.store = LanceDBStore(config=cm.get().lancedb)
        
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
    
    def _import_tbl_format(self, table_name: str, max_workers: int = 4) -> int:
        """Import TBL format (pipe-delimited, with or without partitions)"""
        schema = self.schemas.get(table_name.lower())
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
        
        # Parse files (in parallel if multiple, sequential if single)
        all_records = []
        if len(files_to_import) == 1:
            # Single file - parse directly
            records = self._parse_csv_file(files_to_import[0], schema, delimiter="|")
            all_records.extend(records)
            logger.info(f"Parsed {len(records)} records")
        else:
            # Multiple files - parse in parallel with separate processes
            logger.info(f"Starting {max_workers} worker processes...")
            schema_dict = schema.to_dict() if hasattr(schema, 'to_dict') else {}
            
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(_parse_csv_file_worker, str(f), schema_dict, "|"): f.name
                    for f in files_to_import
                }
                
                for i, future in enumerate(as_completed(futures), 1):
                    file_name = futures[future]
                    try:
                        records = future.result()
                        all_records.extend(records)
                        logger.info(f"  Partition {i}/{len(files_to_import)}: {len(records)} records")
                    except Exception as e:
                        logger.error(f"Error processing {file_name}: {e}")
        
        if not all_records:
            logger.warning(f"No records found for table '{table_name}'")
            return 0
        
        logger.info(f"Total {len(all_records)} records, storing to LanceDB...")
        
        try:
            self.store.upsert(
                records=all_records,
                table_name=table_name,
                pk=schema.pk,
                schema=schema.to_pyarrow_schema(),
            )
            logger.info(f"✓ Imported {len(all_records)} records for '{table_name}'")
            return len(all_records)
        except Exception as e:
            logger.error(f"Error storing {table_name}: {e}")
            return 0
    
    def _import_csv_format(self, table_name: str, max_workers: int = 4) -> int:
        """Import CSV format (comma-delimited, with or without partitions)"""
        schema = self.schemas.get(table_name.lower())
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
        
        # Parse files (in parallel if multiple, sequential if single)
        all_records = []
        if len(files_to_import) == 1:
            # Single file - parse directly
            records = self._parse_csv_file(files_to_import[0], schema, delimiter=",")
            all_records.extend(records)
            logger.info(f"Parsed {len(records)} records")
        else:
            # Multiple files - parse in parallel with separate processes
            logger.info(f"Starting {max_workers} worker processes...")
            schema_dict = schema.to_dict() if hasattr(schema, 'to_dict') else {}
            
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(_parse_csv_file_worker, str(f), schema_dict, ","): f.name
                    for f in files_to_import
                }
                
                for i, future in enumerate(as_completed(futures), 1):
                    file_name = futures[future]
                    try:
                        records = future.result()
                        all_records.extend(records)
                        logger.info(f"  Partition {i}/{len(files_to_import)}: {len(records)} records")
                    except Exception as e:
                        logger.error(f"Error processing {file_name}: {e}")
        
        if not all_records:
            logger.warning(f"No records found for table '{table_name}'")
            return 0
        
        logger.info(f"Total {len(all_records)} records, storing to LanceDB...")
        
        try:
            self.store.upsert(
                records=all_records,
                table_name=table_name,
                pk=schema.pk,
                schema=schema.to_pyarrow_schema(),
            )
            logger.info(f"✓ Imported {len(all_records)} records for '{table_name}'")
            return len(all_records)
        except Exception as e:
            logger.error(f"Error storing {table_name}: {e}")
            return 0
    
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
            if table_name.lower() not in self.schemas:
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
        importer = DataImporter(str(schema_path), str(data_source))
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
