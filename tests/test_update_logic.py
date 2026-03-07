"""
Test to verify UPDATE operation extracts and parses the correct values (after, not before)
"""

import struct
from pixels_lance.proto import sink_pb2
from pixels_lance.fetcher import RowRecordBinaryExtractor
from pixels_lance.parser import DataParser, Schema, SchemaField


def test_update_uses_after_values():
    """
    Verify that UPDATE operations use 'after' values, not 'before' values
    
    Test scenario:
    - before: w_id=1, w_ytd=100.0
    - after:  w_id=1, w_ytd=200.0
    - Expected result: parsed record should have w_ytd=200.0 (after value)
    """
    
    # Create schema with 2 fields for simplicity
    schema = Schema(
        fields=[
            SchemaField("w_id", "int32", size=4, offset=0, nullable=False),
            SchemaField("w_ytd", "float32", size=4, offset=4, nullable=False),
        ],
        table_name="test_warehouse",
        pk=["w_id"]
    )
    
    # Create mock RowRecord with UPDATE operation
    row_record = sink_pb2.RowRecord()
    row_record.op = "UPDATE"
    
    # BEFORE values: w_id=1, w_ytd=100.0
    before_w_id = struct.pack('>i', 1)  # int32 big-endian
    before_w_ytd = struct.pack('>f', 100.0)  # float32 big-endian
    row_record.before.values.add(value=before_w_id)
    row_record.before.values.add(value=before_w_ytd)
    
    # AFTER values: w_id=1, w_ytd=200.0
    after_w_id = struct.pack('>i', 1)  # int32 big-endian
    after_w_ytd = struct.pack('>f', 200.0)  # float32 big-endian
    row_record.after.values.add(value=after_w_id)
    row_record.after.values.add(value=after_w_ytd)
    
    # Extract binary using fetcher
    op_type, column_values = RowRecordBinaryExtractor.extract_row_binary(row_record)
    
    print(f"\nExtracted {len(column_values)} columns for {op_type} operation")
    print(f"Column values: {[v.hex() for v in column_values]}")
    
    # Verify we have 4 columns total (2 before + 2 after)
    assert op_type == "UPDATE"
    assert len(column_values) == 4, f"Expected 4 columns (2 before + 2 after), got {len(column_values)}"
    
    # Parse with DataParser
    parser = DataParser(schema=schema)
    parsed_record = parser.parse(column_values, op_type="UPDATE")
    
    print(f"\nParsed record: {parsed_record}")
    
    # CRITICAL VERIFICATION: The parsed record should use AFTER values (200.0), not BEFORE (100.0)
    assert parsed_record["w_id"] == 1, f"Expected w_id=1, got {parsed_record['w_id']}"
    assert parsed_record["w_ytd"] == 200.0, f"Expected w_ytd=200.0 (after), got {parsed_record['w_ytd']}"
    
    # Verify it's NOT using before value
    assert parsed_record["w_ytd"] != 100.0, "Parser incorrectly used 'before' value instead of 'after'"
    
    print("✅ UPDATE operation correctly uses 'after' values for upsert")


def test_update_column_structure():
    """
    Verify the column structure for UPDATE operations
    """
    schema = Schema(
        fields=[
            SchemaField("id", "int32", size=4, offset=0, nullable=False),
            SchemaField("name", "char", size=10, offset=4, nullable=False),
            SchemaField("value", "float32", size=4, offset=14, nullable=False),
        ],
        table_name="test_table",
        pk=["id"]
    )
    
    row_record = sink_pb2.RowRecord()
    row_record.op = "UPDATE"
    
    # Before: id=1, name="OLD_NAME\x00", value=10.0
    row_record.before.values.add(value=struct.pack('>i', 1))
    row_record.before.values.add(value=b"OLD_NAME\x00\x00")
    row_record.before.values.add(value=struct.pack('>f', 10.0))
    
    # After: id=1, name="NEW_NAME\x00", value=20.0
    row_record.after.values.add(value=struct.pack('>i', 1))
    row_record.after.values.add(value=b"NEW_NAME\x00\x00")
    row_record.after.values.add(value=struct.pack('>f', 20.0))
    
    op_type, column_values = RowRecordBinaryExtractor.extract_row_binary(row_record)
    
    # Verify column structure: [before_0, before_1, before_2, after_0, after_1, after_2]
    assert len(column_values) == 6, f"Expected 6 columns, got {len(column_values)}"
    
    # Verify before values (indices 0-2)
    assert struct.unpack('>i', column_values[0])[0] == 1
    assert column_values[1].decode('utf-8').strip('\x00') == "OLD_NAME"
    assert struct.unpack('>f', column_values[2])[0] == 10.0
    
    # Verify after values (indices 3-5)
    assert struct.unpack('>i', column_values[3])[0] == 1
    assert column_values[4].decode('utf-8').strip('\x00') == "NEW_NAME"
    assert struct.unpack('>f', column_values[5])[0] == 20.0
    
    # Parse and verify final result uses 'after' section
    parser = DataParser(schema=schema)
    parsed = parser.parse(column_values, op_type="UPDATE")
    
    assert parsed["id"] == 1
    assert parsed["name"].strip() == "NEW_NAME", f"Expected 'NEW_NAME', got '{parsed['name']}'"
    assert parsed["value"] == 20.0, f"Expected 20.0 (after), got {parsed['value']}"
    
    print("✅ UPDATE column structure and parsing verified correctly")


if __name__ == "__main__":
    test_update_uses_after_values()
    test_update_column_structure()
    print("\n✅ All UPDATE logic tests passed!")
