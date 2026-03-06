"""
Configuration management for Pixels Lance
"""

import os
from pathlib import Path
from typing import Any, Dict, Optional

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, Field


class RpcConfig(BaseModel):
    """RPC configuration - supports both HTTP-JSON and gRPC"""
    url: str = Field(..., description="RPC endpoint URL (for HTTP-JSON)")
    timeout: int = Field(default=30, description="Request timeout in seconds")
    max_retries: int = Field(default=3, description="Maximum retry attempts")
    batch_size: int = Field(default=100, description="Batch size for RPC polling (records per batch)")
    batch_timeout: int = Field(default=5, description="Max seconds to wait before flushing batch to storage")
    # gRPC specific fields
    grpc_host: Optional[str] = Field(default=None, description="gRPC server host")
    grpc_port: int = Field(default=6688, description="gRPC server port")
    use_grpc: bool = Field(default=False, description="Use gRPC instead of HTTP-JSON")


class LanceDBConfig(BaseModel):
    """LanceDB configuration"""
    db_path: str = Field(default="./lancedb", description="Path to LanceDB database (local path or s3://bucket/path)")
    table_name: str = Field(default="data", description="Table name in LanceDB")
    mode: str = Field(default="overwrite", description="Write mode: overwrite or append")
    # Storage options for object stores (S3, GCS, Azure)
    storage_options: Optional[Dict[str, Any]] = Field(default=None, description="Storage options for object stores")
    # HTTP(S) proxy settings
    proxy: Optional[str] = Field(default=None, description="HTTP(S) proxy URL (e.g., http://proxy.example.com:8080)")


class ParserConfig(BaseModel):
    """Data parser configuration"""
    schema_file: str = Field(..., description="Path to schema definition file")
    encoding: str = Field(default="utf-8", description="Data encoding")


class Config(BaseModel):
    """Main configuration"""
    rpc: RpcConfig
    lancedb: LanceDBConfig
    parser: ParserConfig
    log_level: str = Field(default="INFO", description="Logging level")

    class Config:
        extra = "allow"


class ConfigManager:
    """Manages configuration from YAML and environment files"""

    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize config manager

        Args:
            config_path: Path to config.yaml file. If None, uses default location.
        """
        # Load environment variables from .env
        env_path = Path("config/.env")
        if env_path.exists():
            load_dotenv(env_path)

        # Determine config file path
        if config_path:
            self.config_path = Path(config_path)
        else:
            self.config_path = Path("config/config.yaml")

        self.config = self._load_config()

    def _load_config(self) -> Config:
        """Load configuration from YAML file with environment variable substitution"""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Config file not found: {self.config_path}")

        with open(self.config_path, "r") as f:
            data = yaml.safe_load(f)

        # Substitute environment variables (format: ${VAR_NAME})
        data = self._substitute_env_vars(data)

        return Config(**data)

    def _substitute_env_vars(self, data: Any) -> Any:
        """Recursively substitute environment variables in config"""
        if isinstance(data, dict):
            return {k: self._substitute_env_vars(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._substitute_env_vars(item) for item in data]
        elif isinstance(data, str):
            # Replace ${VAR_NAME:-default} or ${VAR_NAME} with environment variable value
            import re
            def replace_var(match):
                var_with_default = match.group(1)
                # Check if there's a default value specified (VAR_NAME:-default)
                if ':-' in var_with_default:
                    var_name, default_value = var_with_default.split(':-', 1)
                    value = os.getenv(var_name.strip())
                    # Return default if env var is not set or is empty
                    if not value:
                        return default_value if default_value else None
                    return value
                else:
                    # No default value, just get the env var
                    var_name = var_with_default
                    return os.getenv(var_name, match.group(0))
            
            result = re.sub(r'\$\{([^}]+)\}', replace_var, data)
            # If result is None or empty string, return None to indicate no value
            return result if result else None
        return data

    def get(self) -> Config:
        """Get configuration object"""
        return self.config

    def get_dict(self) -> Dict[str, Any]:
        """Get configuration as dictionary"""
        return self.config.dict()
