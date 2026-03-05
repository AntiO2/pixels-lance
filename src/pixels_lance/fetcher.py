"""
RPC data fetcher module
"""

from typing import Iterator, Optional

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
        logger.info("RpcFetcher session closed")
