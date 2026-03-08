#!/usr/bin/env python3
"""
Test script to verify Lance primary key metadata is correctly set
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pixels_lance.parser import Schema, SchemaCollection


def test_primary_key_metadata():
    """Test that primary key metadata is correctly added to PyArrow schema"""
    
    # Test 1: Load hybench schema and check customer table
    print("\n" + "="*80)
    print("Test 1: CH-Benchmark Schema Primary Keys")
    print("="*80)
    
    schema_obj = Schema.from_yaml("config/schema_chbenchmark.yaml")
    if isinstance(schema_obj, SchemaCollection):
        schemas = schema_obj.schemas
    else:
        schemas = {schema_obj.table_name: schema_obj}
    
    for table_name, schema in schemas.items():
        print(f"\nTable: {table_name}")
        print(f"  Primary Key: {schema.pk if schema.pk else 'None'}")
        
        # Convert to PyArrow schema
        pa_schema = schema.to_pyarrow_schema()
        
        # Check for primary key metadata
        if schema.pk:
            for pk_field_name in schema.pk:
                # Find field in PyArrow schema (case-insensitive)
                found = False
                for field in pa_schema:
                    if field.name.lower() == pk_field_name.lower():
                        found = True
                        if field.metadata:
                            pk_meta = field.metadata.get(b"lance-schema:unenforced-primary-key")
                            pos_meta = field.metadata.get(b"lance-schema:unenforced-primary-key:position")
                            
                            if pk_meta:
                                print(f"  ✓ Field '{field.name}': Primary key metadata = {pk_meta.decode()}")
                                if pos_meta:
                                    print(f"    Position: {pos_meta.decode()}")
                                print(f"    Nullable: {field.nullable}")
                            else:
                                print(f"  ✗ Field '{field.name}': Missing primary key metadata!")
                        else:
                            print(f"  ✗ Field '{field.name}': No metadata at all!")
                        break
                
                if not found:
                    print(f"  ✗ Primary key field '{pk_field_name}' not found in schema!")
    
    # Test 2: Load hybench schema
    print("\n" + "="*80)
    print("Test 2: HyBench Schema Primary Keys")
    print("="*80)
    
    schema_obj = Schema.from_yaml("config/schema_hybench.yaml")
    if isinstance(schema_obj, SchemaCollection):
        schemas = schema_obj.schemas
    else:
        schemas = {schema_obj.table_name: schema_obj}
    
    for table_name, schema in schemas.items():
        print(f"\nTable: {table_name}")
        print(f"  Primary Key: {schema.pk if schema.pk else 'None'}")
        
        # Convert to PyArrow schema
        pa_schema = schema.to_pyarrow_schema()
        
        # Check for primary key metadata
        if schema.pk:
            for pk_field_name in schema.pk:
                # Find field in PyArrow schema (case-insensitive)
                found = False
                for field in pa_schema:
                    if field.name.lower() == pk_field_name.lower():
                        found = True
                        if field.metadata:
                            pk_meta = field.metadata.get(b"lance-schema:unenforced-primary-key")
                            pos_meta = field.metadata.get(b"lance-schema:unenforced-primary-key:position")
                            
                            if pk_meta:
                                print(f"  ✓ Field '{field.name}': Primary key metadata = {pk_meta.decode()}")
                                if pos_meta:
                                    print(f"    Position: {pos_meta.decode()}")
                                print(f"    Nullable: {field.nullable}")
                            else:
                                print(f"  ✗ Field '{field.name}': Missing primary key metadata!")
                        else:
                            print(f"  ✗ Field '{field.name}': No metadata at all!")
                        break
                
                if not found:
                    print(f"  ✗ Primary key field '{pk_field_name}' not found in schema!")
    
    print("\n" + "="*80)
    print("Test completed!")
    print("="*80 + "\n")


if __name__ == "__main__":
    test_primary_key_metadata()
