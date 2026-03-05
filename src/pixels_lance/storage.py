"""
LanceDB storage module
"""

from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import lancedb
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
    """Manages data storage in LanceDB"""

    def __init__(
        self,
        config: Optional[LanceDBConfig] = None,
        config_path: Optional[str] = None,
    ):
        """
        Initialize LanceDB store

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

        # Create database directory if it doesn't exist
        Path(self.config.db_path).mkdir(parents=True, exist_ok=True)

        self.db = lancedb.connect(self.config.db_path)
        self.table = None

        logger.info(
            "LanceDBStore initialized",
            extra={
                "db_path": self.config.db_path,
                "table_name": self.config.table_name,
            },
        )

    def create_table(
        self,
        table_name: Optional[str] = None,
        schema: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Create or open a table

        Args:
            table_name: Table name (uses config.table_name if not provided)
            schema: Optional schema definition
        """
        table_name = table_name or self.config.table_name
        self.table_name = table_name

        if table_name in self.db.table_names():
            self.table = self.db.open_table(table_name)
            logger.info("Opened existing table", extra={"table_name": table_name})
        else:
            logger.info("Table does not exist yet, will be created on first write")

    def save(
        self,
        data: Union[Dict[str, Any], List[Dict[str, Any]]],
        table_name: Optional[str] = None,
    ) -> None:
        """
        Save data to LanceDB

        Args:
            data: Single record (dict) or list of records
            table_name: Table name (uses config.table_name if not provided)
        """
        table_name = table_name or self.config.table_name

        # Ensure data is a list
        if isinstance(data, dict):
            data = [data]

        try:
            if self.table is None or self.table.name != table_name:
                # Create table on first write
                if table_name not in self.db.table_names():
                    self.table = self.db.create_table(table_name, data=data)
                    logger.info("Created new table", extra={"table_name": table_name})
                else:
                    self.table = self.db.open_table(table_name)

            # Add data to existing table
            mode = "append"
            if self.config.mode == "overwrite":
                mode = "overwrite"

            if mode == "overwrite" and self.table is not None:
                # For overwrite, we need to drop and recreate
                self.db.drop_table(table_name)
                self.table = self.db.create_table(table_name, data=data)
                logger.info("Overwrote table", extra={"table_name": table_name, "record_count": len(data)})
            else:
                # Append mode
                self.table.add(data)
                logger.info("Saved data", extra={"table_name": table_name, "record_count": len(data)})

        except Exception as e:
            logger.error(
                "Failed to save data",
                extra={"table_name": table_name, "error": str(e)},
            )
            raise

    def upsert(
        self,
        data: Union[Dict[str, Any], List[Dict[str, Any]]],
        table_name: Optional[str] = None,
        key: Optional[Union[str, List[str]]] = None,
    ) -> None:
        """
        Perform merge-insert (upsert) using Lance merge_insert API.

        Args:
            data: record or list of records
            table_name: table name to operate on
            key: primary key field(s) used for matching
        """
        table_name = table_name or self.config.table_name
        if isinstance(data, dict):
            data = [data]

        if key is None and hasattr(self, "table") and self.table is not None:
            # attempt to fetch pk from schema if available
            try:
                key = self.table.schema.primary_key
            except Exception:
                key = None

        # convert to pyarrow table
        pa_table = pa.Table.from_pylist(data)

        # compute dataset path (each table stored individually)
        dataset_path = f"{self.config.db_path}/{table_name}.lance"
        dataset = lance.dataset(dataset_path)

        if key is None:
            raise ValueError("Primary key must be provided for upsert")

        # build merge_insert operation
        op = dataset.merge_insert(key)
        op = op.when_matched_update_all()
        op = op.when_not_matched_insert_all()
        op.execute(pa_table)
        logger.info("Upsert completed", extra={"table_name": table_name, "records": len(data)})

    def delete(
        self,
        data: Union[Dict[str, Any], List[Dict[str, Any]]],
        table_name: Optional[str] = None,
        key: Optional[Union[str, List[str]]] = None,
    ) -> None:
        """
        Delete records from LanceDB table.

        Args:
            data: record or list of records to delete (must contain primary key values)
            table_name: table name to operate on
            key: primary key field(s) used for matching
        """
        table_name = table_name or self.config.table_name
        if isinstance(data, dict):
            data = [data]

        if key is None and hasattr(self, "table") and self.table is not None:
            try:
                key = self.table.schema.primary_key
            except Exception:
                key = None

        if key is None:
            raise ValueError("Primary key must be provided for delete")

        # compute dataset path
        dataset_path = f"{self.config.db_path}/{table_name}.lance"
        dataset = lance.dataset(dataset_path)

        # Build WHERE clause for primary key matching
        if isinstance(key, str):
            # Single primary key
            pk_values = [record[key] for record in data]
            where_clause = f"{key} IN ({', '.join(repr(v) for v in pk_values)})"
        else:
            # Composite primary key - this is more complex, for now assume single key
            raise ValueError("Composite primary keys not yet supported for delete")

        # Execute delete
        dataset.delete(where_clause)
        logger.info("Delete completed", extra={"table_name": table_name, "records": len(data)})

    def query(
        self,
        table_name: Optional[str] = None,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """
        Query data from LanceDB

        Args:
            table_name: Table name
            **kwargs: Query parameters

        Returns:
            List of records
        """
        table_name = table_name or self.config.table_name

        try:
            table = self.db.open_table(table_name)
            results = table.search().to_list()
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
        Get information about a table

        Args:
            table_name: Table name

        Returns:
            Table information
        """
        table_name = table_name or self.config.table_name

        try:
            table = self.db.open_table(table_name)
            return {
                "name": table.name,
                "schema": str(table.schema),
                "record_count": len(table.to_pandas()),
            }
        except Exception as e:
            logger.error(
                "Failed to get table info",
                extra={"table_name": table_name, "error": str(e)},
            )
            return None

    def close(self) -> None:
        """Close database connection"""
        logger.info("Closing LanceDB connection")
