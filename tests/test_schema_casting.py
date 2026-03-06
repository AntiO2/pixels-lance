#!/usr/bin/env python3
"""
Test script to verify PyArrow schema casting from YAML schema definition
"""

import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from pixels_lance.parser import DataParser
from pixels_lance.storage import LanceDBStore
from pixels_lance.config import ConfigManager

def test_schema_to_pyarrow():
    """Test that schema can be converted to PyArrow schema"""
    print("=" * 60)
    print("Testing schema.to_pyarrow_schema() conversion")
    print("=" * 60)
    
    # Load the order table schema
    parser = DataParser(schema_path="config/schema_chbenchmark.yaml", table_name="order")
    
    print(f"\nSchema loaded: {parser.schema.table_name}")
    print(f"Primary key: {parser.schema.pk}")
    print(f"Number of fields: {len(parser.schema.fields)}")
    
    # Convert to PyArrow schema
    pa_schema = parser.schema.to_pyarrow_schema()
    print(f"\nPyArrow schema generated:")
    print(pa_schema)
    
    # Check that nullable fields are properly marked
    print("\nField nullable properties:")
    for field in parser.schema.fields:
        pa_field = pa_schema.field(field.name)
        print(f"  {field.name}: nullable={pa_field.nullable}, type={pa_field.type}")
    
    # Verify o_carrier_id is nullable int32
    o_carrier_field = pa_schema.field("o_carrier_id")
    assert o_carrier_field.nullable is True, "o_carrier_id should be nullable"
    assert str(o_carrier_field.type) == "int32", "o_carrier_id should be int32"
    print("\n✓ o_carrier_id is correctly marked as nullable int32")
    
    return pa_schema

def test_create_table_with_nulls():
    """Test creating a PyArrow table with all NULL values for a nullable field"""
    print("\n" + "=" * 60)
    print("Testing PyArrow table creation with all NULL values")
    print("=" * 60)
    
    import pyarrow as pa
    
    # Load schema
    parser = DataParser(schema_path="config/schema_chbenchmark.yaml", table_name="order")
    pa_schema = parser.schema.to_pyarrow_schema()
    
    # Create test data with all NULLs for o_carrier_id
    test_records = [
        {
            "o_id": 1,
            "o_d_id": 1,
            "o_w_id": 1,
            "o_c_id": 100,
            "o_entry_d": None,  # Will be filled
            "o_carrier_id": None,  # All NULL
            "o_ol_cnt": 2,
            "o_all_local": 1,
            "freshness_ts": None,
        },
        {
            "o_id": 2,
            "o_d_id": 1,
            "o_w_id": 1,
            "o_c_id": 101,
            "o_entry_d": None,
            "o_carrier_id": None,  # All NULL
            "o_ol_cnt": 3,
            "o_all_local": 1,
            "freshness_ts": None,
        },
    ]
    
    # Create table with explicit schema
    table = pa.Table.from_pylist(test_records, schema=pa_schema)
    print(f"\nTable created with {len(table)} rows")
    print(f"Table schema:\n{table.schema}")
    
    # Verify o_carrier_id column type
    carrier_col = table.column("o_carrier_id")
    print(f"\no_carrier_id column type: {carrier_col.type}")
    assert str(carrier_col.type) == "int32", "o_carrier_id should be int32, not null"
    print("✓ o_carrier_id column is correctly typed as int32 (not null type)")
    
    # Now add data with actual int32 values
    print("\nAdding second batch with actual o_carrier_id values...")
    test_records_batch2 = [
        {
            "o_id": 3,
            "o_d_id": 1,
            "o_w_id": 1,
            "o_c_id": 102,
            "o_entry_d": None,
            "o_carrier_id": 5,  # Actual value
            "o_ol_cnt": 1,
            "o_all_local": 1,
            "freshness_ts": None,
        },
    ]
    
    table2 = pa.Table.from_pylist(test_records_batch2, schema=pa_schema)
    print(f"Second batch table schema:\n{table2.schema}")
    print("✓ Second batch created successfully with int32 values")
    
    print("\n✓ Schema casting test PASSED")
    return table, table2

if __name__ == "__main__":
    try:
        print("\nRunning schema casting tests...\n")
        
        # Test 1: Schema to PyArrow conversion
        pa_schema = test_schema_to_pyarrow()
        
        # Test 2: Table creation with NULLs
        table1, table2 = test_create_table_with_nulls()
        
        print("\n" + "=" * 60)
        print("ALL TESTS PASSED ✓")
        print("=" * 60)
        print("\nThe schema.yaml definitions are now properly used for")
        print("PyArrow table creation, ensuring type consistency across")
        print("batches even when some fields are all NULL.")
        
    except Exception as e:
        print(f"\n✗ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
