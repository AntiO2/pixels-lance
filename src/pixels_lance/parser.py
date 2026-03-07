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

try:
    from .logger import get_logger
    from .proto import sink_pb2
except ImportError:
    from logger import get_logger
    from proto import sink_pb2

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
        timezone: Optional[str] = None,
    ):
        """
        Initialize schema field

        Args:
            name: Field name
            type_: Data type (int32, float32, varchar, timestamp, etc.)
            size: Size in bytes (for fixed-size types and strings)
            offset: Offset in binary data (optional, auto-calculated if not provided)
                precision: For decimal and timestamp types (timestamp: 0=s, 3=ms, 6=us, 9=ns)
            scale: For decimal types
            charset: Character encoding for string types
            nullable: Whether field can be null
                timezone: For timestamp types (IANA timezone or '-' for no timezone)
        """
        self.name = name
        self.type = type_
        self.size = size
        self.offset = offset
        self.precision = precision
        self.scale = scale
        self.charset = charset
        self.nullable = nullable
        self.timezone = timezone

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
            "date": 4,
            "time": 4,
            "timestamp": 8,
            "timestamp_with_tz": 8,
            "boolean": 1,
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
                timezone=f.get("timezone"),
            )
            for f in data.get("fields", [])
        ]
        # Support both 'pk' and 'primary_key' field names
        pk = data.get("pk") or data.get("primary_key") or []
        # Ensure pk is always a list
        if isinstance(pk, str):
            pk = [pk]
        return cls(fields, table_name=data.get("table_name"), pk=pk)

    @classmethod
    def from_yaml(cls, path: Union[str, Path]) -> Union["Schema", "SchemaCollection"]:
        """Load schema or collection from YAML file

        If the YAML contains a single schema (dict with fields) this returns a
        `Schema`.  When the file contains a top-level list of schema dicts, or a
        dict with a "schemas" or "tables" key, a ``SchemaCollection`` is returned.  
        This allows benchmarks with multiple tables to share one file.
        """
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        # single schema
        if isinstance(data, dict) and "fields" in data:
            return cls.from_dict(data)

        # multiple schemas (standard format: list or dict with "schemas"/"tables" key)
        items = None
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict) and "schemas" in data:
            items = data["schemas"]
        elif isinstance(data, dict) and "tables" in data:
            # Convert tables dict to list format expected by SchemaCollection
            items = list(data["tables"].values())

        if items is not None:
            return SchemaCollection.from_list(items)

        # fallback: try to interpret as single schema
        return cls.from_dict(data)

    def to_pyarrow_schema(self) -> "pa.Schema":
        """Convert schema to PyArrow schema respecting nullable flags from YAML
        
        This ensures that fields marked as nullable in schema.yaml are properly
        typed as nullable in PyArrow, even if all values are NULL in a batch.
        
        Also adds Lance primary key metadata to field metadata for unenforced primary keys.
        For primary key fields, validates they are non-nullable as required by Lance.
        """
        try:
            import pyarrow as pa
        except ImportError:
            raise ImportError("PyArrow is required for to_pyarrow_schema()")
        
        pa_fields = []
        
        # If primary key is defined, validate and track field positions
        pk_positions = {}
        if self.pk:
            for i, pk_field in enumerate(self.pk, 1):
                pk_positions[pk_field.lower()] = i
        
        for field in self.fields:
            pa_type = self._get_pyarrow_type_for_field(field)
            
            # Build field metadata for Lance primary key support
            metadata = {}
            field_name_lower = field.name.lower()
            
            if field_name_lower in pk_positions:
                # Validate primary key constraints
                if field.nullable:
                    logger.warning(
                        f"Primary key field '{field.name}' is marked as nullable, "
                        f"but Lance requires primary key fields to be non-nullable. "
                        f"Overriding to nullable=False."
                    )
                    field.nullable = False
                
                # Add Lance primary key metadata
                metadata[b"lance-schema:unenforced-primary-key"] = b"true"
                position = pk_positions[field_name_lower]
                metadata[b"lance-schema:unenforced-primary-key:position"] = str(position).encode("utf-8")
            
            pa_field = pa.field(field.name, pa_type, nullable=field.nullable, metadata=metadata)
            pa_fields.append(pa_field)
        
        return pa.schema(pa_fields)
    
    def _get_pyarrow_type_for_field(self, field: SchemaField) -> "pa.DataType":
        """Map schema field to PyArrow type with full metadata support (precision, timezone, etc.)"""
        try:
            import pyarrow as pa
        except ImportError:
            raise ImportError("PyArrow is required for _get_pyarrow_type_for_field()")
        
        field_type = field.type
        
        # Map schema types to PyArrow types
        type_map = {
            "int8": pa.int8(),
            "int16": pa.int16(),
            "int32": pa.int32(),
            "int64": pa.int64(),
            "uint8": pa.uint8(),
            "uint16": pa.uint16(),
            "uint32": pa.uint32(),
            "uint64": pa.uint64(),
            "float32": pa.float32(),
            "float64": pa.float64(),
            "varchar": pa.string(),
            "char": pa.string(),
            "string": pa.string(),
            "bytes": pa.binary(),
            "binary": pa.binary(),
            "varbinary": pa.binary(),
            "date": pa.date32(),
            "time": pa.time64("us"),  # microseconds (only unit time64 supports)
            "boolean": pa.bool_(),
            "decimal": pa.decimal128(38, 10),  # Default precision/scale
        }
        
        # Handle timestamp types with configurable precision and timezone.
        # NOTE:
        # - `timestamp` and `timestamp_with_tz` both support timezone metadata.
        # - If timezone is not provided, default to UTC.
        # - Use timezone '-' to explicitly request timezone-naive timestamp.
        if field_type.lower() in ("timestamp", "timestamp_with_tz"):
            # Map precision to PyArrow unit (3=ms, 6=us, 9=ns, default=ms)
            precision_to_unit = {
                0: "s",   # seconds
                3: "ms",  # milliseconds
                6: "us",  # microseconds
                9: "ns",  # nanoseconds
            }
            unit = precision_to_unit.get(field.precision, "ms")
            
            # Determine timezone
            if field.timezone == "-":
                tz = None
            elif field.timezone:
                tz = field.timezone
            else:
                # Default to UTC when timezone is omitted
                tz = "UTC"
            
            return pa.timestamp(unit, tz=tz)
        
        pa_type = type_map.get(field_type.lower())
        if pa_type is None:
            # Default to string if type not recognized
            logger.warning(f"Unknown field type '{field_type}', defaulting to string")
            return pa.string()
        return pa_type



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

    def parse(self, column_values: List[bytes], op_type: str = "INSERT") -> Dict[str, Any]:
        """
        Parse column values according to schema

        Args:
            column_values: List of binary data, one per column (in schema field order)
                - For INSERT/SNAPSHOT: contains only 'after' columns
                - For UPDATE: contains 'before' columns followed by 'after' columns (2N columns)
                - For DELETE: contains only 'before' columns
            op_type: Operation type - "INSERT", "UPDATE", "DELETE", "SNAPSHOT"

        Returns:
            Dictionary with field names as keys and parsed values (uses 'after' for UPDATE)
        """
        result = {}
        num_fields = len(self.schema.fields)

        # Normalize op_type: support both string and protobuf enum integer values
        if isinstance(op_type, str):
            op_type = op_type.upper()
            # Map string to protobuf enum value for consistent handling
            op_map = {
                "INSERT": sink_pb2.INSERT,
                "UPDATE": sink_pb2.UPDATE,
                "DELETE": sink_pb2.DELETE,
                "SNAPSHOT": sink_pb2.SNAPSHOT,
            }
            op_type = op_map.get(op_type, sink_pb2.INSERT)
        
        # Calculate expected column count based on operation type
        if op_type in (sink_pb2.INSERT, sink_pb2.SNAPSHOT):
            expected_cols = num_fields
            start_idx = 0  # Start from first column
        elif op_type == sink_pb2.UPDATE:
            expected_cols = 2 * num_fields  # before + after
            start_idx = num_fields  # Start from 'after' section (skip 'before')
        elif op_type == sink_pb2.DELETE:
            expected_cols = num_fields  # Only 'before' columns
            start_idx = 0
        else:
            logger.warning(f"Unknown operation type: {op_type}, treating as INSERT")
            expected_cols = num_fields
            start_idx = 0

        # Validate column count
        if len(column_values) != expected_cols:
            field_names = [f.name for f in self.schema.fields]
            column_sizes = [len(c) if c else 0 for c in column_values]
            op_name = sink_pb2.OperationType.Name(op_type) if isinstance(op_type, int) else op_type
            logger.error(
                f"Column count mismatch for operation {op_name}: "
                f"expected {expected_cols} (num_fields={num_fields}, 2x for UPDATE), "
                f"received {len(column_values)}, "
                f"start_idx={start_idx}, "
                f"expected_fields={field_names}, "
                f"column_sizes={column_sizes}"
            )
            # Try to handle gracefully - if we have too many columns, skip extras
            if len(column_values) > expected_cols:
                column_values = column_values[:expected_cols]
            # If we have too few columns, we'll fill in missing values below
            
        # Parse fields from the appropriate section
        for i, field in enumerate(self.schema.fields):
            try:
                col_idx = start_idx + i
                if col_idx < len(column_values):
                    value = self._parse_field_value(column_values[col_idx], field)
                    result[field.name] = value
                else:
                    result[field.name] = None if field.nullable else field.name + "_MISSING"
            except Exception as e:
                op_name = sink_pb2.OperationType.Name(op_type) if isinstance(op_type, int) else op_type
                logger.error(
                    "Failed to parse field",
                    extra={
                        "field": field.name,
                        "operation": op_name,
                        "type": field.type,
                        "column_index": start_idx + i,
                        "data_len": len(column_values[start_idx + i]) if (start_idx + i) < len(column_values) else 0,
                        "error": str(e)
                    },
                )
                result[field.name] = None if field.nullable else field.name + "_ERROR"

        return result

    def _parse_field_value(self, data: bytes, field: SchemaField) -> Any:
        """Parse a single field's binary value (no offset needed, data is the complete field value)"""
        field_type = field.type.lower()

        # Handle integer types
        if field_type in ("int8", "byte"):
            return self._parse_int(data, 0, 1, signed=True)
        elif field_type == "uint8":
            return self._parse_int(data, 0, 1, signed=False)
        elif field_type == "int16":
            return self._parse_int(data, 0, 2, signed=True)
        elif field_type == "uint16":
            return self._parse_int(data, 0, 2, signed=False)
        elif field_type in ("int32", "int"):
            return self._parse_int(data, 0, 4, signed=True)
        elif field_type == "uint32":
            return self._parse_int(data, 0, 4, signed=False)
        elif field_type in ("int64", "bigint", "long"):
            return self._parse_int(data, 0, 8, signed=True)
        elif field_type == "uint64":
            return self._parse_int(data, 0, 8, signed=False)

        # Handle floating point types
        elif field_type in ("float32", "float"):
            return self._parse_float(data, 0, 4)
        elif field_type in ("float64", "double"):
            return self._parse_float(data, 0, 8)

        # Handle string types
        elif field_type in ("varchar", "char", "string"):
            return self._parse_string(data, 0, field.size, field.charset)

        # Handle binary types
        elif field_type in ("bytes", "binary", "varbinary"):
            return self._parse_bytes(data, 0, field.size)

        # Handle date/time types
        elif field_type == "date":
            return self._parse_date(data, 0)
        elif field_type == "time":
            return self._parse_time(data, 0)
        elif field_type in ("timestamp", "timestamp_with_tz"):
            return self._parse_timestamp(data, 0, field.precision or 3)

        # Handle decimal type
        elif field_type == "decimal":
            return self._parse_decimal(data, 0, field.size, field.precision, field.scale)

        # Handle boolean
        elif field_type == "boolean":
            return self._parse_boolean(data, 0)

        else:
            raise ValueError(f"Unsupported type: {field_type}")

    def _parse_int(self, data: bytes, offset: int, size: int, signed: bool = True) -> Optional[int]:
        """Parse integer from binary data (Big-Endian)"""
        if offset + size > len(data):
            return None
        
        field_data = data[offset:offset + size]
        
        if signed:
            fmt = {1: 'b', 2: '>h', 4: '>i', 8: '>q'}.get(size)  # Big-Endian
        else:
            fmt = {1: 'B', 2: '>H', 4: '>I', 8: '>Q'}.get(size)  # Big-Endian
        
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
        """Parse date (days since epoch as int32, Big-Endian)"""
        if offset + 4 > len(data):
            return None
        
        days = struct.unpack('>i', data[offset:offset + 4])[0]  # Big-Endian
        # Convert days since epoch (1970-01-01)
        epoch = datetime(1970, 1, 1).date()
        return epoch + __import__('datetime').timedelta(days=days)

    def _parse_time(self, data: bytes, offset: int) -> Optional[time]:
        """Parse time (milliseconds in a day as int32, Big-Endian)"""
        if offset + 4 > len(data):
            return None
        
        millis = struct.unpack('>i', data[offset:offset + 4])[0]  # Big-Endian
        total_seconds = millis // 1000
        microseconds = (millis % 1000) * 1000
        
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        
        return time(hour=hours, minute=minutes, second=seconds, microsecond=microseconds)

    def _parse_timestamp(self, data: bytes, offset: int, precision: int) -> Optional[datetime]:
        """Parse timestamp (epoch microseconds as int64, Big-Endian)"""
        if offset + 8 > len(data):
            logger.warning(
                "Timestamp data too short",
                extra={"expected": 8, "actual": len(data) - offset, "data_hex": data.hex()}
            )
            return None
        
        value = struct.unpack('>q', data[offset:offset + 8])[0]  # Big-Endian
        
        # The value is in microseconds (not milliseconds)
        try:
            return datetime.fromtimestamp(value / 1000000.0)
        except (ValueError, OSError) as e:
            logger.error(
                "Failed to parse timestamp",
                extra={"value": value, "error": str(e)}
            )
            return None

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

    def parse_batch(self, data_list: List[List[bytes]], op_type: str = "INSERT") -> List[Dict[str, Any]]:
        """
        Parse multiple records (each record is a list of column values)

        Args:
            data_list: List of records, where each record is a list of column binary values
            op_type: Operation type - "INSERT", "UPDATE", "DELETE", "SNAPSHOT"

        Returns:
            List of parsed dictionaries
        """
        return [self.parse(column_values, op_type=op_type) for column_values in data_list]
