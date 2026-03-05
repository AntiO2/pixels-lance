"""
Pixels Lance - RPC Binary Data Fetcher and LanceDB Storage

Supports both HTTP-JSON RPC and gRPC (PixelsPollingService) for binary data fetching.
"""

__version__ = "0.1.0"

# Import core modules that don't require external dependencies
from .config import ConfigManager
from .logger import get_logger, setup_logging

# Import optional modules - they may not be available if dependencies aren't installed
try:
    from .fetcher import RpcFetcher, RowRecordBinaryExtractor
except ImportError:
    RpcFetcher = None
    RowRecordBinaryExtractor = None

try:
    from .grpc_fetcher import PixelsGrpcFetcher, PixelsGrpcFetcherAsync
except ImportError:
    PixelsGrpcFetcher = None
    PixelsGrpcFetcherAsync = None

try:
    from .parser import DataParser
except ImportError:
    DataParser = None

try:
    from .storage import LanceDBStore
except ImportError:
    LanceDBStore = None

__all__ = [
    "ConfigManager",
    "get_logger",
    "setup_logging",
    "RpcFetcher",
    "DataParser",
    "LanceDBStore",
]
