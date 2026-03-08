#!/usr/bin/env python3
"""
Test if lance-schema:unenforced-primary-key metadata is actually created
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pixels_lance.parser import Schema


def test_pk_metadata_creation():
    """Test that primary key metadata is actually added to PyArrow fields"""
    
    schema_dict = {
        "table_name": "test_table",
        "pk": ["custID"],
        "fields": [
            {"name": "custID", "type": "int32", "size": 4, "offset": 0, "nullable": False},
            {"name": "name", "type": "varchar", "size": 20, "offset": 4, "nullable": True},
        ]
    }
    
    schema = Schema.from_dict(schema_dict)
    print(f"Schema pk: {schema.pk}")
    
    pa_schema = schema.to_pyarrow_schema()
    print(f"\nPyArrow schema: {pa_schema}")
    
    print("\n" + "="*80)
    print("Checking field metadata:")
    print("="*80)
    
    for field in pa_schema:
        print(f"\nField: {field.name}")
        print(f"  Type: {field.type}")
        print(f"  Nullable: {field.nullable}")
        print(f"  Metadata: {field.metadata}")
        
        if field.metadata:
            for key, value in field.metadata.items():
                print(f"    {key}: {value}")
        else:
            print("    (no metadata)")
    
    # Verify custID has the metadata
    custid_field = pa_schema.field("custID")
    
    if custid_field.metadata is None:
        print("\n✗ FAIL: custID field has NO metadata at all!")
        return False
    
    if b"lance-schema:unenforced-primary-key" not in custid_field.metadata:
        print("\n✗ FAIL: custID field is missing lance-schema:unenforced-primary-key metadata!")
        print(f"   Available metadata keys: {list(custid_field.metadata.keys())}")
        return False
    
    pk_value = custid_field.metadata[b"lance-schema:unenforced-primary-key"]
    print(f"\n✓ SUCCESS: custID has lance-schema:unenforced-primary-key = {pk_value}")
    
    if b"lance-schema:unenforced-primary-key:position" in custid_field.metadata:
        pos_value = custid_field.metadata[b"lance-schema:unenforced-primary-key:position"]
        print(f"✓ SUCCESS: custID has position = {pos_value}")
    else:
        print("✗ FAIL: custID is missing position metadata!")
        return False
    
    return True


if __name__ == "__main__":
    success = test_pk_metadata_creation()
    sys.exit(0 if success else 1)
