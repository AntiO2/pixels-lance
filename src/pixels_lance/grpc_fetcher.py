"""gRPC client for fetching binary data from PixelsPollingService"""

import logging
from typing import List, Optional
import grpc
from pixels_lance.proto import sink_pb2, sink_pb2_grpc
from pixels_lance.logger import get_logger

logger = get_logger(__name__)


class PixelsGrpcFetcher:
    """gRPC client for fetching row records from Pixels PollingService
    
    Wraps the PixelsPollingService to poll for table changes and
    return row records as binary data.
    """

    def __init__(self, host: str = "localhost", port: int = 6688, timeout: int = 30):
        """
        Initialize gRPC client

        Args:
            host: gRPC server host
            port: gRPC server port
            timeout: Request timeout in seconds
        """
        self.host = host
        self.port = port
        self.timeout = timeout
        self.channel = None
        self.stub = None
        logger.info("PixelsGrpcFetcher initialized", extra={"host": host, "port": port})

    def connect(self) -> None:
        """Establish gRPC channel connection"""
        if self.channel is None:
            target = f"{self.host}:{self.port}"
            self.channel = grpc.aio.secure_channel(
                target,
                grpc.ssl_channel_credentials(),
            ) if self.host != "localhost" else grpc.aio.insecure_channel(target)
            self.stub = sink_pb2_grpc.PixelsPollingServiceStub(self.channel)
            logger.info("gRPC channel connected", extra={"target": target})

    def close(self) -> None:
        """Close gRPC channel"""
        if self.channel is not None:
            # For async channel, properly close it
            self.channel = None
            logger.info("gRPC channel closed")

    def poll_events(
        self,
        schema_name: str,
        table_name: str,
        buckets: Optional[List[int]] = None,
    ) -> List[sink_pb2.RowRecord]:
        """
        Poll events from PixelsPollingService

        Args:
            schema_name: Database/schema name
            table_name: Table name to poll
            buckets: List of bucket IDs to poll (optional)

        Returns:
            List of RowRecord protobuf messages
        """
        if self.stub is None:
            raise RuntimeError("Not connected; call connect() first")

        request = sink_pb2.PollRequest(
            schema_name=schema_name,
            table_name=table_name,
            buckets=buckets or [],
        )

        try:
            response = self.stub.PollEvents(request, timeout=self.timeout)
            logger.info(
                "Polled events",
                extra={
                    "schema": schema_name,
                    "table": table_name,
                    "record_count": len(response.records),
                },
            )
            return response.records
        except grpc.RpcError as e:
            logger.error(
                "gRPC poll failed",
                extra={"code": e.code(), "details": e.details()},
            )
            raise

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class PixelsGrpcFetcherAsync:
    """Async gRPC client for PixelsPollingService
    
    For use in async/await patterns with asyncio.
    """

    def __init__(self, host: str = "localhost", port: int = 6688, timeout: int = 30):
        """
        Initialize async gRPC client

        Args:
            host: gRPC server host
            port: gRPC server port
            timeout: Request timeout in seconds
        """
        self.host = host
        self.port = port
        self.timeout = timeout
        self.channel = None
        self.stub = None
        logger.info("PixelsGrpcFetcherAsync initialized", extra={"host": host, "port": port})

    async def connect(self) -> None:
        """Establish async gRPC channel connection"""
        if self.channel is None:
            target = f"{self.host}:{self.port}"
            if self.host != "localhost":
                self.channel = grpc.aio.secure_channel(
                    target,
                    grpc.ssl_channel_credentials(),
                )
            else:
                self.channel = grpc.aio.insecure_channel(target)
            self.stub = sink_pb2_grpc.PixelsPollingServiceStub(self.channel)
            logger.info("Async gRPC channel connected", extra={"target": target})

    async def close(self) -> None:
        """Close async gRPC channel"""
        if self.channel is not None:
            await self.channel.close()
            self.channel = None
            logger.info("Async gRPC channel closed")

    async def poll_events(
        self,
        schema_name: str,
        table_name: str,
        buckets: Optional[List[int]] = None,
    ) -> List[sink_pb2.RowRecord]:
        """
        Async poll events from PixelsPollingService

        Args:
            schema_name: Database/schema name
            table_name: Table name to poll
            buckets: List of bucket IDs to poll (optional)

        Returns:
            List of RowRecord protobuf messages
        """
        if self.stub is None:
            raise RuntimeError("Not connected; call connect() first")

        request = sink_pb2.PollRequest(
            schema_name=schema_name,
            table_name=table_name,
            buckets=buckets or [],
        )

        try:
            response = await self.stub.PollEvents(request, timeout=self.timeout)
            logger.info(
                "Async polled events",
                extra={
                    "schema": schema_name,
                    "table": table_name,
                    "record_count": len(response.records),
                },
            )
            return response.records
        except grpc.RpcError as e:
            logger.error(
                "Async gRPC poll failed",
                extra={"code": e.code(), "details": e.details()},
            )
            raise

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
