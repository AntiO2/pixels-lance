"""
Enhanced tests for Pixels Lance parser with support for multiple data types
"""

import pytest
from datetime import date, datetime, time
from pathlib import Path
from pixels_lance.parser import Schema, SchemaField, DataParser


class TestSchemaField:
    """Test SchemaField class"""

    def test_field_creation(self):
        field = SchemaField("test", "int32", size=4)
        assert field.name == "test"
        assert field.type == "int32"
        assert field.get_size() == 4

    def test_field_auto_size(self):
        field = SchemaField("test", "int64")
        assert field.get_size() == 8

    def test_field_with_metadata(self):
        field = SchemaField(
            "decimal_field",
            "decimal",
            precision=10,
            scale=2,
        )
        assert field.precision == 10
        assert field.scale == 2


class TestSchema:
    """Test Schema class"""

    def test_schema_creation(self):
        fields = [
            SchemaField("id", "int32"),
            SchemaField("value", "int64"),
        ]
        schema = Schema(fields)
        assert len(schema.fields) == 2
        assert schema.fields[0].offset == 0
        assert schema.fields[1].offset == 4

    def test_schema_with_table_name(self):
        fields = [SchemaField("id", "int32")]
        schema = Schema(fields, table_name="customer")
        assert schema.table_name == "customer"


class TestDataParser:
    """Test DataParser class with various data types"""

    def test_parser_uint32(self):
        field = SchemaField("value", "uint32", offset=0)
        schema = Schema([field])
        parser = DataParser(schema=schema)

        # 0x00000001 in little-endian
        data = b"\x01\x00\x00\x00"
        result = parser.parse(data)
        assert result["value"] == 1

    def test_parser_int32(self):
        field = SchemaField("value", "int32", offset=0)
        schema = Schema([field])
        parser = DataParser(schema=schema)

        # -1 in little-endian int32
        data = b"\xff\xff\xff\xff"
        result = parser.parse(data)
        assert result["value"] == -1

    def test_parser_uint64(self):
        field = SchemaField("value", "uint64", offset=0)
        schema = Schema([field])
        parser = DataParser(schema=schema)

        # 0x0000000000000001 in little-endian
        data = b"\x01\x00\x00\x00\x00\x00\x00\x00"
        result = parser.parse(data)
        assert result["value"] == 1

    def test_parser_float32(self):
        field = SchemaField("value", "float32", offset=0)
        schema = Schema([field])
        parser = DataParser(schema=schema)

        # IEEE 754 float32 for 1.5 (big-endian)
        data = b"\x3f\xc0\x00\x00"
        result = parser.parse(data)
        assert abs(result["value"] - 1.5) < 0.0001

    def test_parser_float64(self):
        field = SchemaField("value", "float64", offset=0)
        schema = Schema([field])
        parser = DataParser(schema=schema)

        # IEEE 754 float64 for 1.5 (big-endian)
        data = b"\x3f\xf8\x00\x00\x00\x00\x00\x00"
        result = parser.parse(data)
        assert abs(result["value"] - 1.5) < 0.0001

    def test_parser_bytes(self):
        field = SchemaField("data", "bytes", size=4, offset=0)
        schema = Schema([field])
        parser = DataParser(schema=schema)

        data = b"\x01\x02\x03\x04"
        result = parser.parse(data)
        assert result["data"] == "01020304"

    def test_parser_string(self):
        field = SchemaField("name", "string", size=10, offset=0)
        schema = Schema([field])
        parser = DataParser(schema=schema)

        data = b"hello\x00\x00\x00\x00\x00"
        result = parser.parse(data)
        assert result["name"] == "hello"

    def test_parser_varchar(self):
        field = SchemaField("name", "varchar", size=15, offset=0)
        schema = Schema([field])
        parser = DataParser(schema=schema)

        data = b"John Smith\x00\x00\x00\x00\x00"
        result = parser.parse(data)
        assert result["name"] == "John Smith"

    def test_parser_date(self):
        field = SchemaField("created", "date", size=4, offset=0)
        schema = Schema([field])
        parser = DataParser(schema=schema)

        # 18993 days since 1970-01-01 = 2022-01-01
        data = b"\xe1\x4a\x00\x00"  # little-endian
        result = parser.parse(data)
        assert isinstance(result["created"], date)

    def test_parser_timestamp(self):
        field = SchemaField("ts", "timestamp", size=8, offset=0, precision=3)
        schema = Schema([field])
        parser = DataParser(schema=schema)

        # Epoch milliseconds for 2022-01-01 00:00:00 UTC = 1641024000000
        data = b"\x00\xc0\x84\x6f\x00\x00\x00\x00"  # little-endian
        result = parser.parse(data)
        assert isinstance(result["ts"], datetime)

    def test_parser_multiple_fields(self):
        fields = [
            SchemaField("id", "int32", offset=0),
            SchemaField("name", "varchar", size=10, offset=4),
            SchemaField("balance", "float32", offset=14),
        ]
        schema = Schema(fields)
        parser = DataParser(schema=schema)

        data = b"\x01\x00\x00\x00John\x00\x00\x00\x00\x00\x3f\xc0\x00\x00"
        result = parser.parse(data)

        assert result["id"] == 1
        assert result["name"] == "John"
        assert abs(result["balance"] - 1.5) < 0.0001

    def test_parser_nullable_field(self):
        field = SchemaField("value", "int32", offset=100, nullable=True)
        schema = Schema([field])
        parser = DataParser(schema=schema)

        # Short data, field is out of bounds
        data = b"\x00" * 50
        result = parser.parse(data)
        assert result["value"] is None

    def test_parser_batch(self):
        field = SchemaField("value", "uint32", offset=0)
        schema = Schema([field])
        parser = DataParser(schema=schema)

        data_list = [
            b"\x01\x00\x00\x00",
            b"\x02\x00\x00\x00",
            b"\x03\x00\x00\x00",
        ]

        results = parser.parse_batch(data_list)
        assert len(results) == 3
        assert results[0]["value"] == 1
        assert results[1]["value"] == 2
        assert results[2]["value"] == 3


class TestSchemaFromYAML:
    """Test loading schema from YAML files"""

    def test_load_customer_schema(self):
        schema_path = Path(__file__).parent.parent / "config" / "schema_customer.yaml"
        if schema_path.exists():
            schema = Schema.from_yaml(schema_path)
            assert schema.table_name == "customer"
            assert len(schema.fields) > 0
            # Verify first field
            assert schema.fields[0].name == "custID"
            assert schema.fields[0].type == "int32"

    def test_load_transfer_schema(self):
        schema_path = Path(__file__).parent.parent / "config" / "schema_transfer.yaml"
        if schema_path.exists():
            schema = Schema.from_yaml(schema_path)
            assert schema.table_name == "transfer"
            assert schema.fields[0].name == "id"
            assert schema.fields[0].type == "int64"

    def test_load_multiple_schemas_from_single_file(self, tmp_path):
        # construct a temporary YAML file with two schemas
        multi = tmp_path / "multi.yaml"
        content = {
            "schemas": [
                {"table_name": "a", "fields": [{"name": "x", "type": "int32", "size": 4, "offset": 0}]},
                {"table_name": "b", "fields": [{"name": "y", "type": "int64", "size": 8, "offset": 0}]},
            ]
        }
        import yaml
        with open(multi, "w") as f:
            yaml.safe_dump(content, f)

        from pixels_lance.parser import SchemaCollection
        loaded = Schema.from_yaml(str(multi))
        assert isinstance(loaded, SchemaCollection)
        assert "a" in loaded.schemas
        assert "b" in loaded.schemas
        # ensure DataParser can select one
        from pixels_lance.parser import DataParser
        parser_a = DataParser(schema_path=str(multi), table_name="a")
        assert parser_a.schema.table_name == "a"
        parser_b = DataParser(schema_path=str(multi), table_name="b")
        assert parser_b.schema.table_name == "b"

    def test_load_hybench_schema_file(self):
        # using the provided hybench example file in config
        schema_path = Path(__file__).parent.parent / "config" / "schema_hybench.yaml"
        if not schema_path.exists():
            pytest.skip("hybench schema not present")
        loaded = Schema.from_yaml(schema_path)
        # should produce a collection
        from pixels_lance.parser import SchemaCollection
        assert isinstance(loaded, SchemaCollection)
        # check some tables exist and pk
        assert "customer" in loaded.schemas
        assert loaded.schemas["customer"].pk == ["custID"]
        # ensure DataParser selects a table
        parser = DataParser(schema_path=schema_path, table_name="company")
        assert parser.schema.table_name == "company"

    def test_hybench_style_conversion(self, tmp_path):
        # create a fake hybench style YAML with tables key
        hy = tmp_path / "hy.yaml"
        import yaml

        content = {
            "tables": [
                {"name": "one", "pk": ["a"], "fields": {"a": "int", "b": "varchar(5)"}},
                {"name": "two", "fields": {"x": "bigint"}},
            ]
        }
        with open(hy, "w") as f:
            yaml.safe_dump(content, f)

        loaded = Schema.from_yaml(str(hy))
        assert isinstance(loaded, SchemaCollection)
        assert "one" in loaded.schemas and "two" in loaded.schemas
        assert loaded.schemas["one"].fields[0].name == "a"
        assert loaded.schemas["one"].fields[1].type == "varchar"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
