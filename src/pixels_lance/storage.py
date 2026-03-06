"""
Lance storage module
"""

from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import lance
import pyarrow as pa

try:
    from .config import ConfigManager, LanceDBConfig
    from .logger import get_logger
except ImportError:
    from config import ConfigManager, LanceDBConfig
    from logger import get_logger

logger = get_logger(__name__)


class LanceDBStore:
    """Manages data storage using Lance format"""

    def __init__(
        self,
        config: Optional[LanceDBConfig] = None,
        config_path: Optional[str] = None,
    ):
        """
        Initialize Lance store

        Args:
            config: LanceDBConfig object. If None, loads from config_path
            config_path: Path to config.yaml file
        """
        if config:
            self.config = config
        elif config_path:
            cm = ConfigManager(config_path)
            self.config = cm.get().lancedb
        else:
            cm = ConfigManager()
            self.config = cm.get().lancedb

        # Base path for datasets
        self.base_path = self.config.db_path
        
        # Determine if using object store (S3, GCS, Azure)
        is_object_store = self.base_path.startswith(('s3://', 'gs://', 'az://'))
        
        # Create database directory if it's a local path
        if not is_object_store:
            Path(self.base_path).mkdir(parents=True, exist_ok=True)

        # Store storage options for later use
        if self.config.storage_options:
            # Filter out None values from storage_options
            self.storage_options = {k: v for k, v in self.config.storage_options.items() if v is not None and v != ''}
        else:
            self.storage_options = {}

        logger.info(
            "LanceStore initialized",
            extra={
                "base_path": self.base_path,
                "table_name": self.config.table_name,
                "storage_options": list(self.storage_options.keys()) if self.storage_options else [],
            },
        )
        
        self.table = None
        self.table_name = None

    def _get_dataset_path(self, table_name: str) -> str:
        """Get the full path for a dataset"""
        # Remove trailing slash from base_path to avoid double slashes
        base = self.base_path.rstrip('/')
        return f"{base}/{table_name}.lance"

    def create_table(
        self,
        table_name: Optional[str] = None,
        schema: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Create or open a Lance dataset

        Args:
            table_name: Table name (uses config.table_name if not provided)
            schema: Optional schema definition
        """
        table_name = table_name or self.config.table_name
        self.table_name = table_name
        dataset_path = self._get_dataset_path(table_name)

        try:
            # Try to open existing dataset
            if self.storage_options:
                self.table = lance.dataset(dataset_path, storage_options=self.storage_options)
            else:
                self.table = lance.dataset(dataset_path)
            logger.info("Opened existing dataset", extra={"table_name": table_name})
        except Exception:
            logger.info("Dataset does not exist yet, will be created on first write")
            self.table = None

    def save(
        self,
        data: Union[Dict[str, Any], List[Dict[str, Any]]],
        table_name: Optional[str] = None,
    ) -> None:
        """
        Save data to Lance dataset

        Args:
            data: Single record (dict) or list of records
            table_name: Table name (uses config.table_name if not provided)
        """
        table_name = table_name or self.config.table_name
        dataset_path = self._get_dataset_path(table_name)

        # Ensure data is a list
        if isinstance(data, dict):
            data = [data]

        try:
            # Convert to PyArrow Table
            pa_table = pa.Table.from_pylist(data)

            # Check if dataset exists
            dataset_exists = False
            try:
                if self.storage_options:
                    lance.dataset(dataset_path, storage_options=self.storage_options)
                else:
                    lance.dataset(dataset_path)
                dataset_exists = True
            except Exception:
                pass

            if not dataset_exists:
                # Create new dataset
                if self.storage_options:
                    lance.write_dataset(pa_table, dataset_path, storage_options=self.storage_options)
                else:
                    lance.write_dataset(pa_table, dataset_path)
                logger.info("Created new dataset", extra={"table_name": table_name, "record_count": len(data)})
            else:
                # Append or overwrite
                if self.config.mode == "overwrite":
                    if self.storage_options:
                        lance.write_dataset(pa_table, dataset_path, mode="overwrite", storage_options=self.storage_options)
                    else:
                        lance.write_dataset(pa_table, dataset_path, mode="overwrite")
                    logger.info("Overwrote dataset", extra={"table_name": table_name, "record_count": len(data)})
                else:
                    # Append mode - read existing, concatenate, write
                    if self.storage_options:
                        existing = lance.dataset(dataset_path, storage_options=self.storage_options)
                    else:
                        existing = lance.dataset(dataset_path)
                    
                    # Concatenate tables
                    combined = pa.concat_tables([existing.to_table(), pa_table])
                    
                    if self.storage_options:
                        lance.write_dataset(combined, dataset_path, mode="overwrite", storage_options=self.storage_options)
                    else:
                        lance.write_dataset(combined, dataset_path, mode="overwrite")
                    logger.info("Appended to dataset", extra={"table_name": table_name, "record_count": len(data)})

            # Update cached reference
            if self.storage_options:
                self.table = lance.dataset(dataset_path, storage_options=self.storage_options)
            else:
                self.table = lance.dataset(dataset_path)
            self.table_name = table_name

        except Exception as e:
            logger.error(
                "Failed to save data",
                extra={"table_name": table_name, "error": str(e)},
            )
            raise

    def upsert(
        self,
        records: Union[Dict[str, Any], List[Dict[str, Any]]],
        table_name: Optional[str] = None,
        pk: Optional[Union[str, List[str]]] = None,
    ) -> None:
        """
        Perform merge-insert (upsert) using Lance merge_insert API.

        Args:
            records: record or list of records
            table_name: table name to operate on
            pk: primary key field(s) used for matching
        """
        table_name = table_name or self.config.table_name
        dataset_path = self._get_dataset_path(table_name)
        
        if isinstance(records, dict):
            records = [records]

        if pk is None:
            raise ValueError("Primary key must be provided for upsert")

        # Convert to pyarrow table
        pa_table = pa.Table.from_pylist(records)

        # Check if dataset exists
        dataset_exists = False
        try:
            if self.storage_options:
                lance.dataset(dataset_path, storage_options=self.storage_options)
            else:
                lance.dataset(dataset_path)
            dataset_exists = True
        except Exception:
            pass

        if not dataset_exists:
            # Dataset doesn't exist, create it with initial data
            logger.info(f"Dataset does not exist, creating new dataset at {dataset_path}")
            if self.storage_options:
                lance.write_dataset(pa_table, dataset_path, storage_options=self.storage_options)
            else:
                lance.write_dataset(pa_table, dataset_path)
            logger.info("Upsert completed (created new dataset)", extra={"table_name": table_name, "records": len(records)})
        else:
            # Dataset exists, perform merge_insert
            if self.storage_options:
                dataset = lance.dataset(dataset_path, storage_options=self.storage_options)
            else:
                dataset = lance.dataset(dataset_path)

            # Build merge_insert operation
            op = dataset.merge_insert(pk)
            op = op.when_matched_update_all()
            op = op.when_not_matched_insert_all()
            op.execute(pa_table)
            
            logger.info("Upsert completed", extra={"table_name": table_name, "records": len(records)})

    def delete(
        self,
        records: Union[Dict[str, Any], List[Dict[str, Any]]],
        table_name: Optional[str] = None,
        pk: Optional[Union[str, List[str]]] = None,
    ) -> None:
        """
        Delete records from Lance dataset.

        Args:
            records: record or list of records to delete (must contain primary key values)
            table_name: table name to operate on
            pk: primary key field(s) used for matching
        """
        table_name = table_name or self.config.table_name
        dataset_path = self._get_dataset_path(table_name)
        
        if isinstance(records, dict):
            records = [records]

        if pk is None:
            raise ValueError("Primary key must be provided for delete")

        # Load dataset
        if self.storage_options:
            dataset = lance.dataset(dataset_path, storage_options=self.storage_options)
        else:
            dataset = lance.dataset(dataset_path)

        # Build WHERE clause for primary key matching
        if isinstance(pk, str):
            # Single primary key
            pk_values = [record[pk] for record in records]
            where_clause = f"{pk} IN ({', '.join(repr(v) for v in pk_values)})"
        else:
            # Composite primary key - this is more complex, for now assume single key
            raise ValueError("Composite primary keys not yet supported for delete")

        # Execute delete
        dataset.delete(where_clause)
        logger.info("Delete completed", extra={"table_name": table_name, "records": len(records)})

    def query(
        self,
        table_name: Optional[str] = None,
        limit: Optional[int] = None,
        filter: Optional[str] = None,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """
        Query data from Lance dataset

        Args:
            table_name: Table name
            limit: Maximum number of records to return
            filter: SQL-like filter expression
            **kwargs: Additional query parameters

        Returns:
            List of records
        """
        table_name = table_name or self.config.table_name
        dataset_path = self._get_dataset_path(table_name)

        try:
            # Load dataset
            if self.storage_options:
                dataset = lance.dataset(dataset_path, storage_options=self.storage_options)
            else:
                dataset = lance.dataset(dataset_path)
            
            # Convert to table and then to list of dicts
            scanner = dataset.scanner(filter=filter, limit=limit, **kwargs)
            results = scanner.to_table().to_pylist()
            
            logger.info("Queried data", extra={"table_name": table_name, "record_count": len(results)})
            return results
        except Exception as e:
            logger.error(
                "Failed to query data",
                extra={"table_name": table_name, "error": str(e)},
            )
            return []

    def get_table_info(self, table_name: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Get information about a Lance dataset

        Args:
            table_name: Table name

        Returns:
            Table information
        """
        table_name = table_name or self.config.table_name
        dataset_path = self._get_dataset_path(table_name)

        try:
            # Load dataset
            if self.storage_options:
                dataset = lance.dataset(dataset_path, storage_options=self.storage_options)
            else:
                dataset = lance.dataset(dataset_path)
            
            return {
                "name": table_name,
                "schema": str(dataset.schema),
                "record_count": dataset.count_rows(),
                "version": dataset.version,
            }
        except Exception as e:
            logger.error(
                "Failed to get table info",
                extra={"table_name": table_name, "error": str(e)},
            )
            return None

    def close(self) -> None:
        """Close database connection (no-op for Lance)"""
        logger.info("Closing Lance storage")
