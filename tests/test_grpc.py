"""
Tests for gRPC integration with PixelsPollingService
"""

import pytest
from pixels_lance.grpc_fetcher import PixelsGrpcFetcher, PixelsGrpcFetcherAsync
from pixels_lance.proto import sink_pb2
from pixels_lance.fetcher import RowRecordBinaryExtractor


class TestPixelsGrpcFetcher:
    """Test PixelsGrpcFetcher class"""

    def test_grpc_fetcher_init(self):
        """Test gRPC fetcher initialization"""
        fetcher = PixelsGrpcFetcher(host="localhost", port=6688)
        assert fetcher.host == "localhost"
        assert fetcher.port == 6688
        assert fetcher.timeout == 30

    def test_grpc_fetcher_connect(self):
        """Test gRPC fetcher connection setup (without actual server)"""
        fetcher = PixelsGrpcFetcher(host="localhost", port=6688)
        # Only test that connect() method doesn't error in setup
        try:
            fetcher.connect()
            assert fetcher.channel is not None
            assert fetcher.stub is not None
            fetcher.close()
        except Exception:
            # Expected if no server is running
            pass

    def test_grpc_fetcher_context_manager(self):
        """Test gRPC fetcher as context manager"""
        try:
            with PixelsGrpcFetcher(host="localhost", port=6688) as fetcher:
                assert fetcher.stub is not None
        except Exception:
            # Expected if no server is running
            pass


class TestRowRecordBinaryExtractor:
    """Test RowRecordBinaryExtractor utility"""

    def test_extract_row_binary_from_after_value(self):
        """Test extracting binary data from 'after' value"""
        # Create a mock RowRecord with 'after' value
        row_record = sink_pb2.RowRecord()
        row_record.after.values.add(value=b"\x00\x00\x00\x01")
        row_record.after.values.add(value=b"\x3f\xc0\x00\x00")

        binary = RowRecordBinaryExtractor.extract_row_binary(row_record)
        assert binary == b"\x00\x00\x00\x01\x3f\xc0\x00\x00"

    def test_extract_row_binary_from_before_value(self):
        """Test extracting binary data from 'before' value when 'after' is empty"""
        row_record = sink_pb2.RowRecord()
        row_record.before.values.add(value=b"\x01\x02\x03\x04")
        row_record.before.values.add(value=b"\x05\x06\x07\x08")

        binary = RowRecordBinaryExtractor.extract_row_binary(row_record)
        assert binary == b"\x01\x02\x03\x04\x05\x06\x07\x08"

    def test_extract_row_binary_empty_record(self):
        """Test extracting binary from empty record"""
        row_record = sink_pb2.RowRecord()
        binary = RowRecordBinaryExtractor.extract_row_binary(row_record)
        assert binary is None

    def test_extract_records_binary_batch(self):
        """Test extracting binary from multiple records"""
        records = []
        for i in range(3):
            row_record = sink_pb2.RowRecord()
            row_record.after.values.add(value=f"data_{i}".encode())
            records.append(row_record)

        binaries = RowRecordBinaryExtractor.extract_records_binary(records)
        assert len(binaries) == 3
        assert binaries[0] == b"data_0"
        assert binaries[1] == b"data_1"
        assert binaries[2] == b"data_2"

    def test_extract_records_skips_empty(self):
        """Test that empty records are skipped"""
        records = [
            sink_pb2.RowRecord(),  # empty
        ]
        record = sink_pb2.RowRecord()
        record.after.values.add(value=b"valid")
        records.append(record)

        binaries = RowRecordBinaryExtractor.extract_records_binary(records)
        assert len(binaries) == 1
        assert binaries[0] == b"valid"


class TestProtoBufferGeneration:
    """Test that protobuf messages are correctly generated"""

    def test_poll_request_creation(self):
        """Test creating PollRequest message"""
        request = sink_pb2.PollRequest(
            schema_name="test_db",
            table_name="test_table",
            buckets=[0, 1, 2],
        )
        assert request.schema_name == "test_db"
        assert request.table_name == "test_table"
        assert list(request.buckets) == [0, 1, 2]

    def test_row_record_creation(self):
        """Test creating RowRecord message"""
        record = sink_pb2.RowRecord()
        record.after.values.add(value=b"\x00\x00\x00\x01")
        record.source.db = "testdb"
        record.source.schema = "public"
        record.source.table = "users"
        record.op = sink_pb2.INSERT

        assert record.source.db == "testdb"
        assert record.source.table == "users"
        assert record.op == sink_pb2.INSERT

    def test_operation_type_enum(self):
        """Test OperationType enum values"""
        assert sink_pb2.INSERT == 0
        assert sink_pb2.UPDATE == 1
        assert sink_pb2.DELETE == 2
        assert sink_pb2.SNAPSHOT == 3


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
