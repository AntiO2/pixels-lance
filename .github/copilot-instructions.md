# Pixels Lance - AI Agent Instructions

## Project Overview

**Pixels Lance** is a Python framework for fetching binary data from RPC endpoints, parsing it according to configurable schemas, and storing it in LanceDB. Features advanced binary deserialization inspired by Apache Flink's changelog patterns, supporting multiple data types and benchmark configurations.

## Architecture

### Core Components

1. **RpcFetcher** (`src/pixels_lance/fetcher.py`)
   - Manages RPC connections and data fetching
   - Supports single and batch requests
   - Handles retries and timeouts
   - Returns binary data or hex strings

2. **DataParser** (`src/pixels_lance/parser.py`) - **Enhanced Edition**
   - Advanced binary data parsing inspired by Flink's ChangelogRowRecordDeserializer
   - **Comprehensive type support**: int8/16/32/64, uint8/16/32/64, float32/64, varchar, char, string, bytes, binary, varbinary, date, time, timestamp, timestamp_with_tz, decimal, boolean
   - **Automatic offset calculation** and variable-length field handling
   - **Big-endian and little-endian** support
   - Null value handling and error recovery
   - Batch parsing with per-field error isolation

3. **LanceDBStore** (`src/pixels_lance/storage.py`)
   - Handles data persistence in LanceDB
   - Supports create, append, and overwrite modes
   - Manages table schemas automatically
   - Provides query capabilities

4. **ConfigManager** (`src/pixels_lance/config.py`)
   - Loads YAML configuration with environment variable substitution
   - Validates configuration using Pydantic
   - Supports `.env` files for sensitive data
   - Three separate config sections: RPC, LanceDB, Parser

## Configuration Pattern

The project uses **separated configuration** as a core principle with **per-benchmark schemas**:

- **config/config.yaml**: Main configuration (RPC URL, LanceDB path, batch size, log level)
- **config/schema_*.yaml**: Benchmark-specific binary data schemas (one per table/benchmark)
  - `schema_customer.yaml`: Customer table (114 bytes, 18 fields)
  - `schema_company.yaml`: Company table (209 bytes, 15 fields)
  - `schema_savingAccount.yaml`: Saving account table (32 bytes, 6 fields)
  - `schema_checkingAccount.yaml`: Checking account table (32 bytes, 6 fields)
  - `schema_transfer.yaml`: Transfer table (46 bytes, 7 fields)
  - `schema_checking.yaml`: Checking table (42 bytes, 7 fields)
  - `schema_loanapps.yaml`: Loan applications (44 bytes, 7 fields)
  - `schema_loantrans.yaml`: Loan transactions (60 bytes, 10 fields)
- **config/.env.example**: Template for environment variables (copy to `.env`)

Environment variables in config files use `${VAR_NAME}` syntax with optional defaults: `${VAR_NAME:-default_value}`

## Development Workflow

### Setup
```bash
pip install -e ".[dev]"  # Install with dev dependencies
```

### Testing
```bash
pytest tests/ -v --cov=src
```

### Code Quality
```bash
black src/ tests/          # Format code
flake8 src/ tests/         # Lint
mypy src/                  # Type check
```

### Running with Benchmark Schema
```bash
# Load customer benchmark
pixels-lance --config config/config.yaml --schema config/schema_customer.yaml

# Or programmatically (see examples.py)
```

## Key Patterns

### Schema Definition (Per-Benchmark)
Each benchmark has its own YAML schema file in `config/schema_*.yaml`. You can also
**group multiple table schemas into a single YAML file**; the parser will return a
`SchemaCollection` and you pick a table by name when initializing `DataParser`.

```yaml
# single schema example
table_name: customer
fields:
  - name: custID
    type: int32          # Signed 32-bit integer
    size: 4
    offset: 0
    nullable: false
    description: "Customer ID"

  - name: name
    type: varchar        # Variable-length string
    size: 15             # Max size in bytes
    offset: 8
    charset: utf-8
    nullable: true

  - name: loan_balance
    type: float32        # IEEE 754 single-precision
    size: 4
    offset: 74
    nullable: true

  - name: created_date
    type: date           # Days since epoch (int32)
    size: 4
    offset: 94

  - name: ts
    type: timestamp      # Epoch milliseconds (int64)
    size: 8
    precision: 3         # Millisecond precision
    offset: 98

record_size: 114         # Total binary record size
```

### Supported Data Types

| Type | Size | Description |
|------|------|-------------|
| `int8`, `int16`, `int32`, `int64` | 1,2,4,8 | Signed integers |
| `uint8`, `uint16`, `uint32`, `uint64` | 1,2,4,8 | Unsigned integers |
| `float32`, `float64` | 4,8 | IEEE 754 floating point |
| `varchar`, `char`, `string` | Variable | UTF-8 strings |
| `bytes`, `binary`, `varbinary` | Variable | Hex-encoded binary data |
| `date` | 4 | Days since 1970-01-01 |
| `time` | 4 | Milliseconds in a day |
| `timestamp`, `timestamp_with_tz` | 8 | Epoch milliseconds |
| `decimal` | Variable | High-precision decimal with precision/scale |
| `boolean` | 1 | True (1) / False (0) |

### Binary Parsing Example

```python
from pixels_lance import DataParser, ConfigManager

# Load customer schema
parser = DataParser(schema_path="config/schema_customer.yaml")

# Parse binary data
binary_data = b'\x00\x00\x00\x01...'  # 114 bytes for customer
result = parser.parse(binary_data)

# Access parsed fields
customer_id = result['custID']        # int32
name = result['name']                 # str
balance = result['loan_balance']      # float
created = result['created_date']      # date object
ts = result['ts']                     # datetime object
```

### Batch Processing Multiple Records

```python
records = parser.parse_batch(data_list)  # List[bytes] -> List[Dict]
store.save(records, table_name="customer")
```

### Adding Custom Data Types

Extend `DataParser._parse_field()` in `src/pixels_lance/parser.py`:

```python
def _parse_field(self, data: bytes, field: SchemaField) -> Any:
    # ... existing types ...
    elif field_type == "my_custom_type":
        return self._parse_custom_type(data, offset, field.size)

def _parse_custom_type(self, data: bytes, offset: int, size: int):
    # Custom parsing logic
    return custom_result
```

### Extending RPC Support
- Subclass `RpcFetcher` for custom RPC implementations
- Override `fetch()` or `fetch_batch()` methods as needed

### Custom Storage Backends
- Subclass `LanceDBStore` to implement alternative storage
- Override `save()` and `query()` methods

## File Structure Priority

When working with this codebase, focus on:
1. **config/schema_*.yaml** - Per-benchmark schema definitions (SQL table to binary mapping)
2. **src/pixels_lance/parser.py** - Core binary deserialization logic
3. **src/pixels_lance/fetcher.py** - RPC data retrieval
4. **src/pixels_lance/storage.py** - LanceDB persistence
5. **src/pixels_lance/config.py** - Configuration management

## Common Tasks

### Add Support for New Benchmark/Table

1. Get the SQL CREATE TABLE definition
2. Analyze field types and calculate binary offsets (packed format)
3. Create `config/schema_<table_name>.yaml` with field definitions
4. Verify field offsets match expected record size
5. Test with `DataParser(schema_path="config/schema_<table_name>.yaml")`

Example offset calculation:
```
custID (int32): offset=0, size=4
companyID (int32): offset=4, size=4
gender (varchar[6]): offset=8, size=6
name (varchar[15]): offset=14, size=15
age (int32): offset=29, size=4
...
Total: 114 bytes
```

### Parse RPC Binary Response

1. Call `RpcFetcher.fetch(method_name, params)` to get binary data
2. Check for `"error"` in response
3. Pass result to `DataParser.parse(data)` with appropriate schema
4. Save parsed result to LanceDB via `LanceDBStore.save()`

### Debug Binary Parsing Issues

1. Use `DataParser.parse()` with known test data
2. Check field offsets match actual binary layout
3. Add field to schema incrementally and verify offsets
4. Enable debug logging: `log_level: DEBUG` in config.yaml
5. Use `logger.debug()` to trace parsing of specific fields

## Dependencies

**Core**: lancedb, requests, pydantic, python-dotenv, pyyaml
**Dev**: pytest, pytest-cov

## Testing Strategy

- Unit tests in `tests/test_parser.py`
- Test each data type parsing separately
- Verify offset calculations with record size
- Mock RPC responses for integration tests
- Validate configuration loading with environment variables

## Logging

Uses Python's `structlog`-inspired JSON logging. Set `log_level` in config.yaml or CLI:
```bash
pixels-lance --config config/config.yaml --log-level DEBUG
```

## Entry Points

- **CLI**: `pixels-lance` command with `--schema` parameter
- **Module**: `from pixels_lance import DataParser, RpcFetcher, LanceDBStore`
- **Examples**: See `examples.py` for common patterns

## Special Notes on Data Types

### Timestamp Fields
- **Input format**: Epoch milliseconds (int64, big-endian)
- **Output format**: Python `datetime` object
- **Precision**: Specify in schema (`precision: 3` for milliseconds, `6` for microseconds)

### Float/Double Fields
- **Storage**: IEEE 754 binary format (big-endian)
- **Types**: `float32` or `float64`
- **Parsing**: Uses struct.unpack with big-endian byte order

### Date Fields
- **Storage**: Days since 1970-01-01 as int32 (big-endian)
- **Output**: Python `date` object

### String/Varchar Fields
- **Storage**: UTF-8 encoded bytes
- **Padding**: Null-terminated or fixed-size with null padding
- **Max size**: Specified in schema as `size` field
