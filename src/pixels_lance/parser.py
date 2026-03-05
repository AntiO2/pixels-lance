"""
Enhanced Data parser module for binary data with support for multiple data types
Inspired by Apache Flink's ChangelogRowRecordDeserializer pattern
"""

import struct
from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from io import BytesIO

import yaml

from .logger import get_logger

logger = get_logger(__name__)


class DataType(Enum):
    """Supported data types with their byte representations"""
    # Integer types
    INT8 = "int8"
    INT16 = "int16"
    INT32 = "int32"
    INT64 = "int64"
    UINT8 = "uint8"
    UINT16 = "uint16"
    UINT32 = "uint32"
    UINT64 = "uint64"
    
    # Floating point types
    FLOAT32 = "float32"
    FLOAT64 = "float64"
    
    # String/Text types
    VARCHAR = "varchar"
    CHAR = "char"
    STRING = "string"
    
    # Binary types
    BYTES = "bytes"
    BINARY = "binary"
    VARBINARY = "varbinary"
    
    # Date/Time types
    DATE = "date"
    TIME = "time"
    TIMESTAMP = "timestamp"
    TIMESTAMP_WITH_TZ = "timestamp_with_tz"
    
    # Numeric types
    DECIMAL = "decimal"
    
    # Boolean
    BOOLEAN = "boolean"
    
    # Complex types
    ROW = "row"
    MAP = "map"
    ARRAY = "array"


class SchemaField:
    """Represents a field in the binary data schema with enhanced type support"""


def _convert_hybench_table(table: dict) -> dict:
    """Helper converting hybench style table dict to our schema dict

    The hybench format from `schema_hybench.yaml` uses fields as a key->type
    mapping and includes `pk`.  We transform it to the flat list expected by
    ``Schema.from_dict`` with type translation.
    """
    def map_type(sql_type: str) -> str:
        # basic mapping from SQL style to parser types
        t = sql_type.lower()
        if t.startswith("varchar"):
            return "varchar"
        if t.startswith("char"):
            return "char"
        if t == "int":
            return "int32"
        if t == "bigint":
            return "int64"
        if t == "real":
            return "float32"
        if t == "timestamp":
            return "timestamp"
        if t == "date":
            return "date"
        if t == "int8" or t == "tinyint":
            return "int8"
        if t == "int16" or t == "smallint":
            return "int16"
        if t == "boolean" or t == "bool":
            return "boolean"
        # fallback: return raw
        return t

    fields = []
    offset = 0
    for name, sql_type in table.get("fields", {}).items():
        dtype = map_type(sql_type)
        size = None
        # extract size if varchar or char
        if dtype in ("varchar", "char"):
            # find number inside parentheses
            import re
            m = re.search(r"\((\d+)\)", sql_type)
            if m:
                size = int(m.group(1))
        # simple sizing for fixed types
        schema_field = {
            "name": name,
            "type": dtype,
            "size": size,
            "offset": offset,
            "nullable": True,
        }
        # advance offset with approximate size
        if size is not None:
            offset += size
        else:
            # default sizes
            default_sizes = {"int32": 4, "int64": 8, "float32": 4, "date": 4, "timestamp": 8}
            offset += default_sizes.get(dtype, 0)
        fields.append(schema_field)
    return {"table_name": table.get("name"), "fields": fields, "pk": table.get("pk", [])}


    def __init__(
        self,
        name: str,
        type_: str,
        size: Optional[int] = None,
        offset: Optional[int] = None,
        precision: Optional[int] = None,
        scale: Optional[int] = None,
        charset: str = "utf-8",
        nullable: bool = True,
    ):
        """
        Initialize schema field

        Args:
            name: Field name
            type_: Data type (int32, float32, varchar, timestamp, etc.)
            size: Size in bytes (for fixed-size types and strings)
            offset: Offset in binary data (optional, auto-calculated if not provided)
            precision: For decimal types
            scale: For decimal types
            charset: Character encoding for string types
            nullable: Whether field can be null
        """
        self.name = name
        self.type = type_
        self.size = size
        self.offset = offset
        self.precision = precision
        self.scale = scale
        self.charset = charset
        self.nullable = nullable

    def get_size(self) -> int:
        """Get size of this field in bytes"""
        if self.size is not None:
            return self.size

        # Auto-calculate size based on type
        type_sizes = {
            "int8": 1,
            "uint8": 1,
            "int16": 2,
            "uint16": 2,
            "int32": 4,
            "uint32": 4,
            "float32": 4,
            "int64": 8,
            "uint64": 8,
            "float64": 8,
            "date": 4,  # Days since epoch (int32)
            "time": 4,  # Milliseconds in a day (int32)
            "timestamp": 8,  # Epoch milliseconds (int64)
        }

        return type_sizes.get(self.type, 0)


class Schema:
    """Enhanced schema definition for binary data with support for SQL table structures"""

    def __init__(self, fields: List[SchemaField], table_name: Optional[str] = None, pk: Optional[List[str]] = None):
        """Initialize schema with fields
        
        Args:
            fields: List of SchemaField objects
            table_name: Optional table name for identification
            pk: Optional list of primary key field names
        """
        self.fields = fields
        self.table_name = table_name
        self.pk = pk or []
        self._calculate_offsets()

    def _calculate_offsets(self) -> None:
        """Auto-calculate offsets if not specified"""
        offset = 0
        for field in self.fields:
            if field.offset is None:
                field.offset = offset
            offset = max(offset, field.offset + field.get_size())

    @classmethod
    def from_dict(cls, data: dict) -> "Schema":
        """Create schema from dictionary"""
        fields = [
            SchemaField(
                name=f["name"],
                type_=f["type"],
                size=f.get("size"),
                offset=f.get("offset"),
                precision=f.get("precision"),
                scale=f.get("scale"),
                charset=f.get("charset", "utf-8"),
                nullable=f.get("nullable", True),
            )
            for f in data.get("fields", [])
        ]
        pk = data.get("pk") or []
        return cls(fields, table_name=data.get("table_name"), pk=pk)

    @classmethod
    def from_yaml(cls, path: Union[str, Path]) -> Union["Schema", "SchemaCollection"]:
        """Load schema or collection from YAML file

        If the YAML contains a single schema (dict with fields) this returns a
        `Schema`.  When the file contains a top-level list of schema dicts, or a
        dict with a "schemas" key, a ``SchemaCollection`` is returned.  This
        allows benchmarks with multiple tables to share one file.
        """
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        # single schema
        if isinstance(data, dict) and "fields" in data:
            return cls.from_dict(data)

        # multiple schemas
        items = None
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict) and "schemas" in data:
            items = data["schemas"]
        # hybench style uses 'tables' as the key for multiple definitions
        elif isinstance(data, dict) and "tables" in data:
            items = data["tables"]

        if items is not None:
            # detect hybench style (each item has name, pk, fields mapping)
            hybench = False
            for item in items:
                if "fields" in item and isinstance(item["fields"], dict):
                    hybench = True
                    break
            if hybench:
                return SchemaCollection.from_list([_convert_hybench_table(i) for i in items])
            return SchemaCollection.from_list(items)

        # fallback: try to interpret as single schema
        return cls.from_dict(data)



class SchemaCollection:
    """Container for multiple `Schema` objects keyed by table name.

    Useful when a single YAML file holds several benchmark tables. The
    collection is essentially a mapping from `table_name` to `Schema`.
    """

    def __init__(self, schemas: Dict[str, Schema]):
        self.schemas = schemas

    @classmethod
    def from_list(cls, data_list: List[dict]) -> "SchemaCollection":
        """Build a collection from a list of schema dictionaries"""
        schemas: Dict[str, Schema] = {}
        for item in data_list:
            schema = Schema.from_dict(item)
            if schema.table_name is None:
                raise ValueError("Each schema in a collection must have a table_name")
            schemas[schema.table_name] = schema
        return cls(schemas)

    def get(self, table_name: str) -> Schema:
        """Return the schema for the given table_name"""
        return self.schemas[table_name]


class DataParser:
    """Enhanced binary data parser with support for complex types and CDC patterns
    
    Inspired by Apache Flink's deserialization patterns with support for:
    - Multiple numeric types (int8-64, uint8-64, float, double)
    - String types with character set support
    - Date/Time types with multiple formats
    - Decimal types with precision/scale
    - NULL value handling
    """

    def __init__(
        self,
        schema: Optional[Schema] = None,
        schema_path: Optional[Union[str, Path]] = None,
        table_name: Optional[str] = None,
    ):
        """
        Initialize data parser

        Args:
            schema: Schema object. If None, loads from schema_path
            schema_path: Path to schema YAML file
        """
        if schema:
            self.schema = schema
        elif schema_path:
            loaded = Schema.from_yaml(schema_path)
            # if a collection was returned, pick table
            if isinstance(loaded, SchemaCollection):
                if table_name is None:
                    raise ValueError(
                        "schema_path contains multiple tables; specify table_name"
                    )
                self.schema = loaded.get(table_name)
            else:
                self.schema = loaded
        else:
            self.schema = Schema([])

        logger.info("DataParser initialized", extra={"field_count": len(self.schema.fields)})

    def parse(self, data: bytes) -> Dict[str, Any]:
        """
        Parse binary data according to schema

        Args:
            data: Binary data to parse

        Returns:
            Dictionary with field names as keys and parsed values
        """
        result = {}

        for field in self.schema.fields:
            try:
                value = self._parse_field(data, field)
                result[field.name] = value
            except Exception as e:
                logger.error(
                    "Failed to parse field",
                    extra={"field": field.name, "type": field.type, "error": str(e)},
                )
                result[field.name] = None if field.nullable else field.name + "_ERROR"

        return result

    def _parse_field(self, data: bytes, field: SchemaField) -> Any:
        """Parse a single field from binary data with type-aware conversion"""
        offset = field.offset
        
        # Check bounds
        if offset < 0 or offset >= len(data):
            if field.nullable:
                return None
            raise ValueError(f"Offset {offset} out of bounds for field {field.name}")

        field_type = field.type.lower()

        # Handle integer types
        if field_type in ("int8", "byte"):
            return self._parse_int(data, offset, 1, signed=True)
        elif field_type == "uint8":
            return self._parse_int(data, offset, 1, signed=False)
        elif field_type == "int16":
            return self._parse_int(data, offset, 2, signed=True)
        elif field_type == "uint16":
            return self._parse_int(data, offset, 2, signed=False)
        elif field_type in ("int32", "int"):
            return self._parse_int(data, offset, 4, signed=True)
        elif field_type == "uint32":
            return self._parse_int(data, offset, 4, signed=False)
        elif field_type in ("int64", "bigint", "long"):
            return self._parse_int(data, offset, 8, signed=True)
        elif field_type == "uint64":
            return self._parse_int(data, offset, 8, signed=False)

        # Handle floating point types
        elif field_type in ("float32", "float"):
            return self._parse_float(data, offset, 4)
        elif field_type in ("float64", "double"):
            return self._parse_float(data, offset, 8)

        # Handle string types
        elif field_type in ("varchar", "char", "string"):
            return self._parse_string(data, offset, field.size, field.charset)

        # Handle binary types
        elif field_type in ("bytes", "binary", "varbinary"):
            return self._parse_bytes(data, offset, field.size)

        # Handle date/time types
        elif field_type == "date":
            return self._parse_date(data, offset)
        elif field_type == "time":
            return self._parse_time(data, offset)
        elif field_type in ("timestamp", "timestamp_with_tz"):
            return self._parse_timestamp(data, offset, field.precision or 3)

        # Handle decimal type
        elif field_type == "decimal":
            return self._parse_decimal(data, offset, field.size, field.precision, field.scale)

        # Handle boolean
        elif field_type == "boolean":
            return self._parse_boolean(data, offset)

        else:
            raise ValueError(f"Unsupported type: {field_type}")

    def _parse_int(self, data: bytes, offset: int, size: int, signed: bool = True) -> Optional[int]:
        """Parse integer from binary data"""
        if offset + size > len(data):
            return None
        
        field_data = data[offset:offset + size]
        
        if signed:
            fmt = {1: 'b', 2: '<h', 4: '<i', 8: '<q'}.get(size)
        else:
            fmt = {1: 'B', 2: '<H', 4: '<I', 8: '<Q'}.get(size)
        
        if fmt is None:
            raise ValueError(f"Unsupported integer size: {size}")
        
        return struct.unpack(fmt, field_data)[0]

    def _parse_float(self, data: bytes, offset: int, size: int) -> Optional[float]:
        """Parse floating point from binary data (Big Endian)"""
        if offset + size > len(data):
            return None
        
        field_data = data[offset:offset + size]
        fmt = {4: '>f', 8: '>d'}.get(size)  # Big Endian
        
        if fmt is None:
            raise ValueError(f"Unsupported float size: {size}")
        
        return struct.unpack(fmt, field_data)[0]

    def _parse_string(self, data: bytes, offset: int, size: Optional[int], charset: str) -> Optional[str]:
        """Parse string from binary data"""
        if offset >= len(data):
            return None
        
        if size is None:
            # Variable length - read until null terminator
            end = data.find(b'\x00', offset)
            if end == -1:
                end = len(data)
            field_data = data[offset:end]
        else:
            # Fixed length
            if offset + size > len(data):
                field_data = data[offset:]
            else:
                field_data = data[offset:offset + size]
        
        # Decode and remove null terminators
        return field_data.decode(charset, errors='ignore').rstrip('\x00')

    def _parse_bytes(self, data: bytes, offset: int, size: Optional[int]) -> Optional[str]:
        """Parse binary data as hex string"""
        if offset >= len(data):
            return None
        
        if size is None:
            field_data = data[offset:]
        else:
            if offset + size > len(data):
                field_data = data[offset:]
            else:
                field_data = data[offset:offset + size]
        
        return field_data.hex()

    def _parse_date(self, data: bytes, offset: int) -> Optional[date]:
        """Parse date (days since epoch as int32)"""
        if offset + 4 > len(data):
            return None
        
        days = struct.unpack('<i', data[offset:offset + 4])[0]
        # Convert days since epoch (1970-01-01)
        epoch = datetime(1970, 1, 1).date()
        return epoch + __import__('datetime').timedelta(days=days)

    def _parse_time(self, data: bytes, offset: int) -> Optional[time]:
        """Parse time (milliseconds in a day as int32)"""
        if offset + 4 > len(data):
            return None
        
        millis = struct.unpack('<i', data[offset:offset + 4])[0]
        total_seconds = millis // 1000
        microseconds = (millis % 1000) * 1000
        
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        
        return time(hour=hours, minute=minutes, second=seconds, microsecond=microseconds)

    def _parse_timestamp(self, data: bytes, offset: int, precision: int) -> Optional[datetime]:
        """Parse timestamp (epoch milliseconds as int64)"""
        if offset + 8 > len(data):
            return None
        
        millis = struct.unpack('<q', data[offset:offset + 8])[0]
        
        # Convert based on precision
        # precision 3 = milliseconds, 6 = microseconds, etc.
        if precision <= 3:
            return datetime.fromtimestamp(millis / 1000)
        else:
            microseconds = millis % 1000000
            seconds = millis // 1000000
            return datetime.fromtimestamp(seconds + microseconds / 1000000)

    def _parse_decimal(self, data: bytes, offset: int, size: Optional[int], 
                      precision: Optional[int], scale: Optional[int]) -> Optional[Decimal]:
        """Parse decimal from binary (as string or bytes)"""
        if offset >= len(data):
            return None
        
        if size is None:
            size = len(data) - offset
        
        if offset + size > len(data):
            return None
        
        field_data = data[offset:offset + size]
        
        # Try parsing as string first
        try:
            decimal_str = field_data.decode('utf-8', errors='ignore').strip()
            return Decimal(decimal_str)
        except:
            # If string parsing fails, treat as raw bytes (big-endian integer)
            value = int.from_bytes(field_data, byteorder='big', signed=True)
            if scale and scale > 0:
                return Decimal(value) / Decimal(10 ** scale)
            return Decimal(value)

    def _parse_boolean(self, data: bytes, offset: int) -> Optional[bool]:
        """Parse boolean (0=false, 1=true)"""
        if offset >= len(data):
            return None
        
        # Try as single byte
        if offset + 1 <= len(data):
            value = data[offset]
            return value != 0
        
        return False

    def parse_batch(self, data_list: List[bytes]) -> List[Dict[str, Any]]:
        """
        Parse multiple binary data items

        Args:
            data_list: List of binary data

        Returns:
            List of parsed dictionaries
        """
        return [self.parse(data) for data in data_list]
