"""
Test timestamp precision and timezone handling
"""
import pytest
from datetime import datetime
import pyarrow as pa
from pixels_lance.parser import Schema, SchemaField


class TestTimestampTypes:
    """Test timestamp type handling with different precisions and timezones"""

    def test_timestamp_milliseconds_no_tz(self):
        """Test timestamp with millisecond precision and no timezone"""
        schema = Schema(
            fields=[
                SchemaField(
                    name="ts",
                    type_="timestamp",
                    size=8,
                    precision=3,  # milliseconds
                    nullable=False,
                )
            ]
        )
        
        pa_schema = schema.to_pyarrow_schema()
        pa_type = pa_schema.field("ts").type
        
        assert isinstance(pa_type, pa.TimestampType)
        assert pa_type.unit == "ms"
        assert pa_type.tz is None

    def test_timestamp_microseconds_utc(self):
        """Test timestamp with microsecond precision and UTC timezone"""
        schema = Schema(
            fields=[
                SchemaField(
                    name="ts",
                    type_="timestamp",
                    size=8,
                    precision=6,  # microseconds
                    timezone="UTC",
                    nullable=False,
                )
            ]
        )
        
        pa_schema = schema.to_pyarrow_schema()
        pa_type = pa_schema.field("ts").type
        
        assert isinstance(pa_type, pa.TimestampType)
        assert pa_type.unit == "us"
        assert pa_type.tz == "UTC"

    def test_timestamp_nanoseconds_with_tz(self):
        """Test timestamp with nanosecond precision and America/New_York timezone"""
        schema = Schema(
            fields=[
                SchemaField(
                    name="ts",
                    type_="timestamp",
                    size=8,
                    precision=9,  # nanoseconds
                    timezone="America/New_York",
                    nullable=False,
                )
            ]
        )
        
        pa_schema = schema.to_pyarrow_schema()
        pa_type = pa_schema.field("ts").type
        
        assert isinstance(pa_type, pa.TimestampType)
        assert pa_type.unit == "ns"
        assert pa_type.tz == "America/New_York"

    def test_timestamp_seconds(self):
        """Test timestamp with second precision"""
        schema = Schema(
            fields=[
                SchemaField(
                    name="ts",
                    type_="timestamp",
                    size=8,
                    precision=0,  # seconds
                    nullable=False,
                )
            ]
        )
        
        pa_schema = schema.to_pyarrow_schema()
        pa_type = pa_schema.field("ts").type
        
        assert isinstance(pa_type, pa.TimestampType)
        assert pa_type.unit == "s"

    def test_timestamp_with_tz_default_utc(self):
        """Test timestamp_with_tz defaults to UTC if no timezone specified"""
        schema = Schema(
            fields=[
                SchemaField(
                    name="ts",
                    type_="timestamp_with_tz",
                    size=8,
                    precision=3,
                    nullable=False,
                )
            ]
        )
        
        pa_schema = schema.to_pyarrow_schema()
        pa_type = pa_schema.field("ts").type
        
        assert isinstance(pa_type, pa.TimestampType)
        assert pa_type.tz == "UTC"

    def test_timestamp_explicit_no_tz(self):
        """Test timestamp with explicit '-' for no timezone"""
        schema = Schema(
            fields=[
                SchemaField(
                    name="ts",
                    type_="timestamp",
                    size=8,
                    precision=6,
                    timezone="-",  # Explicitly no timezone
                    nullable=False,
                )
            ]
        )
        
        pa_schema = schema.to_pyarrow_schema()
        pa_type = pa_schema.field("ts").type
        
        assert isinstance(pa_type, pa.TimestampType)
        assert pa_type.tz is None

    def test_timestamp_from_yaml_dict(self):
        """Test loading timestamp configuration from dict (simulating YAML)"""
        data = {
            "table_name": "test_table",
            "fields": [
                {
                    "name": "ts_ms_utc",
                    "type": "timestamp",
                    "size": 8,
                    "precision": 3,
                    "timezone": "UTC",
                    "nullable": False,
                },
                {
                    "name": "ts_us_none",
                    "type": "timestamp",
                    "size": 8,
                    "precision": 6,
                    "timezone": "-",
                    "nullable": False,
                },
                {
                    "name": "ts_ns_ny",
                    "type": "timestamp",
                    "size": 8,
                    "precision": 9,
                    "timezone": "America/New_York",
                    "nullable": False,
                },
            ]
        }
        
        schema = Schema.from_dict(data)
        pa_schema = schema.to_pyarrow_schema()
        
        # Check first field: ms + UTC
        ts1 = pa_schema.field("ts_ms_utc").type
        assert isinstance(ts1, pa.TimestampType)
        assert ts1.unit == "ms"
        assert ts1.tz == "UTC"
        
        # Check second field: us + no timezone
        ts2 = pa_schema.field("ts_us_none").type
        assert isinstance(ts2, pa.TimestampType)
        assert ts2.unit == "us"
        assert ts2.tz is None
        
        # Check third field: ns + America/New_York
        ts3 = pa_schema.field("ts_ns_ny").type
        assert isinstance(ts3, pa.TimestampType)
        assert ts3.unit == "ns"
        assert ts3.tz == "America/New_York"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
