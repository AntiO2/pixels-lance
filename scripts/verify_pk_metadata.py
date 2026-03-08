#!/usr/bin/env python3
"""
Verify that primary key metadata is persisted in Lance datasets
"""

import sys
import os
from pathlib import Path
import lance
import yaml
from dotenv import load_dotenv

def get_storage_options():
    """Load storage options from config.yaml and .env"""
    try:
        # Load .env file first
        env_path = Path(__file__).parent.parent / "config" / ".env"
        if env_path.exists():
            load_dotenv(env_path)
            print(f"Loaded environment from {env_path}")
        
        config_path = Path(__file__).parent.parent / "config" / "config.yaml"
        with open(config_path, 'r') as f:
            config_text = f.read()
        
        # Replace environment variables in config
        import re
        def replace_env_var(match):
            var_name = match.group(1)
            default = match.group(2) if match.group(2) else ''
            return os.environ.get(var_name, default)
        
        config_text = re.sub(r'\$\{([^:}]+)(?::-(.*?))?\}', replace_env_var, config_text)
        config = yaml.safe_load(config_text)
        
        storage_opts = config.get('lancedb', {}).get('storage_options', {})
        # Filter out None values and proxy (which causes issues)
        valid_keys = {'region', 'access_key_id', 'secret_access_key', 'endpoint', 'bucket_name'}
        return {k: v for k, v in storage_opts.items() 
                if v is not None and v != '' and k in valid_keys}
    except Exception as e:
        print(f"Warning: Could not load storage options from config: {e}")
        return {}

def verify_dataset_pk(dataset_path: str):
    """Verify primary key metadata in a Lance dataset"""
    try:
        # Get storage options for S3/cloud access
        storage_options = get_storage_options()
        
        if dataset_path.startswith(('s3://', 'gs://', 'az://')):
            print(f"Using storage options: {list(storage_options.keys())}")
            ds = lance.dataset(dataset_path, storage_options=storage_options)
        else:
            ds = lance.dataset(dataset_path)
        
        schema = ds.schema
        
        print(f"\nDataset: {dataset_path}")
        print(f"Total fields: {len(schema)}")
        print("\nField metadata:")
        print("=" * 80)
        
        has_pk = False
        for field in schema:
            metadata = field.metadata if field.metadata else {}
            
            if b"lance-schema:unenforced-primary-key" in metadata:
                has_pk = True
                pk_value = metadata[b"lance-schema:unenforced-primary-key"].decode()
                pos_value = metadata.get(b"lance-schema:unenforced-primary-key:position", b"").decode()
                
                print(f"✓ Field '{field.name}':")
                print(f"  - Type: {field.type}")
                print(f"  - Nullable: {field.nullable}")
                print(f"  - Primary key: {pk_value}")
                if pos_value:
                    print(f"  - Position: {pos_value}")
                print()
        
        if not has_pk:
            print("⚠ No primary key metadata found in this dataset")
        
        print("=" * 80)
        return has_pk
    except Exception as e:
        print(f"✗ Error reading dataset: {e}")
        return False


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 verify_pk_metadata.py <dataset_path>")
        print("\nExamples:")
        print("  python3 verify_pk_metadata.py lancedb/customer.lance")
        print("  python3 verify_pk_metadata.py s3://home-zinuo/lancedb/chbench_wh10000")
        print("  python3 verify_pk_metadata.py s3://home-zinuo/lancedb/chbench_wh10000/warehouse.lance")
        sys.exit(1)
    
    dataset_path = sys.argv[1]
    
    # For local paths, check existence
    if not dataset_path.startswith(('s3://', 'gs://', 'az://')):
        if not Path(dataset_path).exists():
            print(f"Error: Dataset not found: {dataset_path}")
            sys.exit(1)
    
    success = verify_dataset_pk(dataset_path)
    sys.exit(0 if success else 1)
