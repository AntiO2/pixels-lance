"""
RPC data fetcher module

Supports both HTTP-JSON RPC and gRPC endpoints for fetching binary data.
"""

from typing import Iterator, Optional, Union, List

import requests

from .config import ConfigManager, RpcConfig
from .logger import get_logger

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
    def extract_row_binary(row_record) -> Optional[bytes]:
        """
        Extract binary data from a single RowRecord

        Args:
            row_record: sink_pb2.RowRecord protobuf message

        Returns:
            Concatenated binary data of row values, or None if empty
        """
        # Extract 'after' value if available (new/updated data), else 'before'
        row_value = row_record.after if row_record.after and row_record.after.values else row_record.before
        if not row_value or not row_value.values:
            return None

        # Concatenate all column values as bytes
        binary_data = b""
        for col_value in row_value.values:
            binary_data += col_value.value

        return binary_data if binary_data else None

    @staticmethod
    def extract_records_binary(row_records: List) -> List[bytes]:
        """
        Extract binary data from multiple RowRecords

        Args:
            row_records: List of sink_pb2.RowRecord messages

        Returns:
            List of binary data, skipping None entries
        """
        result = []
        for record in row_records:
            binary = RowRecordBinaryExtractor.extract_row_binary(record)
            if binary is not None:
                result.append(binary)
        return result
        logger.info("RpcFetcher session closed")
