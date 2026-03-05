"""
RPC data fetcher module

Supports both HTTP-JSON RPC and gRPC endpoints for fetching binary data.
"""

from typing import Iterator, Optional, Union, List

import requests

try:
    from .config import ConfigManager, RpcConfig
    from .logger import get_logger
except ImportError:
    from config import ConfigManager, RpcConfig
    from logger import get_logger

logger = get_logger(__name__)


class RpcFetcher:
    """Fetches binary data from RPC endpoint"""

    def __init__(
        self,
        config: Optional[RpcConfig] = None,
        config_path: Optional[str] = None,
    ):
        """
        Initialize RPC Fetcher

        Args:
            config: RpcConfig object. If None, loads from config_path
            config_path: Path to config.yaml file
        """
        if config:
            self.config = config
        elif config_path:
            cm = ConfigManager(config_path)
            self.config = cm.get().rpc
        else:
            cm = ConfigManager()
            self.config = cm.get().rpc

        self.session = requests.Session()
        logger.info("RpcFetcher initialized", extra={"url": self.config.url})

    def fetch(
        self,
        method: str,
        params: Optional[dict] = None,
        **kwargs
    ) -> Optional[bytes]:
        """
        Fetch data from RPC endpoint

        Args:
            method: RPC method name
            params: Parameters for the RPC method
            **kwargs: Additional arguments passed to requests

        Returns:
            Binary response data
        """
        params = params or {}
        
        payload = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": 1,
        }

        try:
            response = self.session.post(
                self.config.url,
                json=payload,
                timeout=self.config.timeout,
                **kwargs
            )
            response.raise_for_status()

            data = response.json()
            
            if "error" in data:
                logger.error("RPC error", extra={"error": data["error"], "method": method})
                return None

            if "result" in data:
                # Convert hex string to bytes if needed
                result = data["result"]
                if isinstance(result, str) and result.startswith("0x"):
                    return bytes.fromhex(result[2:])
                return result

            return None

        except requests.RequestException as e:
            logger.error("RPC request failed", extra={"error": str(e), "method": method})
            return None

    def fetch_batch(
        self,
        method: str,
        param_list: list,
        **kwargs
    ) -> Iterator[Optional[bytes]]:
        """
        Fetch multiple items in batch

        Args:
            method: RPC method name
            param_list: List of parameter sets
            **kwargs: Additional arguments passed to fetch

        Yields:
            Binary data for each request
        """
        for params in param_list:
            data = self.fetch(method, params, **kwargs)
            yield data

    def close(self) -> None:
        """Close the session"""
        self.session.close()


class RowRecordBinaryExtractor:
    """Extract binary data from protobuf RowRecord messages
    
    Converts PixelsPollingService responses (RowRecord protobuf messages)
    into binary format suitable for binary data parser.
    """

    @staticmethod
    def extract_row_binary(row_record) -> Optional[tuple]:
        """
        Extract column values and operation type from a single RowRecord

        Args:
            row_record: sink_pb2.RowRecord protobuf message

        Returns:
            Tuple of (operation_type, column_values_list), or None if no data
            column_values_list is a list of bytes, one per column
        """
        # Extract 'after' value if available (new/updated data), else 'before'
        row_value = row_record.after if row_record.after and row_record.after.values else row_record.before
        if not row_value or not row_value.values:
            return None

        # Extract each column value as separate bytes
        column_values = [col_value.value for col_value in row_value.values]

        return (row_record.op, column_values) if column_values else None

    @staticmethod
    def extract_records_binary(row_records: List) -> List[tuple]:
        """
        Extract column values and operation types from multiple RowRecords

        Args:
            row_records: List of sink_pb2.RowRecord messages

        Returns:
            List of (operation_type, column_values_list) tuples, skipping None entries
        """
        result = []
        for record in row_records:
            extracted = RowRecordBinaryExtractor.extract_row_binary(record)
            if extracted is not None:
                result.append(extracted)
        return result
        logger.info("RpcFetcher session closed")
