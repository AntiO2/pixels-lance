"""
Test script for querying customer data from Lance dataset
"""

import lance
import sys
from pathlib import Path

# ========== Configuration ==========
# Set the table name to query
QUERY_TABLE = "customer"
# ===================================

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from pixels_lance.config import ConfigManager


def test_query_customer():
    """Query customer table data"""
    
    # Load configuration from config.yaml
    config_manager = ConfigManager()
    config = config_manager.config
    
    # Construct dataset path from config
    base_path = config.lancedb.db_path
    dataset_path = f"{base_path}/{QUERY_TABLE}.lance"
    
    # Prepare storage options for S3
    storage_options = None
    if base_path.startswith('s3://'):
        storage_options = config.lancedb.storage_options or {}
        # Add proxy if configured
        if config.lancedb.proxy:
            storage_options['proxy_options'] = config.lancedb.proxy
    
    print(f"Querying table: {QUERY_TABLE}")
    print(f"Dataset path: {dataset_path}")
    if storage_options:
        print(f"Storage options: {list(storage_options.keys())}")
    print()
    
    try:
        # Load dataset (with S3 support)
        dataset = lance.dataset(dataset_path, storage_options=storage_options)
        
        print("=" * 70)
        print("Dataset Information")
        print("=" * 70)
        print(f"Path: {dataset_path}")
        print(f"Total rows: {dataset.count_rows()}")
        print(f"Version: {dataset.version}")
        print(f"\nSchema:")
        for field in dataset.schema:
            print(f"  - {field.name}: {field.type}")
        
        print("\n" + "=" * 70)
        print("First 10 Records")
        print("=" * 70)
        
        # Query first 10 records
        scanner = dataset.scanner(limit=10)
        table = scanner.to_table()
        df = table.to_pandas()
        
        # Display with better formatting
        import pandas as pd
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', 30)
        print(df)
        
        print("\n" + "=" * 70)
        print("Statistics")
        print("=" * 70)
        print(f"Total records displayed: {len(df)}")
        print(f"Columns: {len(df.columns)}")
        
        # Show some column statistics
        if 'custID' in df.columns:
            print(f"\ncustID range: {df['custID'].min()} - {df['custID'].max()}")
        if 'age' in df.columns:
            print(f"Age range: {df['age'].min()} - {df['age'].max()}")
        if 'loan_balance' in df.columns:
            print(f"Loan balance range: {df['loan_balance'].min():.2f} - {df['loan_balance'].max():.2f}")
        
        # Freshness timestamp analysis
        if 'freshness_ts' in df.columns:
            print(f"\n{'=' * 70}")
            print("Freshness Timestamp Analysis")
            print(f"{'=' * 70}")
            print(f"Earliest freshness_ts: {df['freshness_ts'].min()}")
            print(f"Latest freshness_ts:   {df['freshness_ts'].max()}")
            
            # Show all unique freshness_ts values
            unique_ts = df['freshness_ts'].unique()
            print(f"Unique freshness_ts values: {len(unique_ts)}")
            if len(unique_ts) <= 10:
                for ts in sorted(unique_ts):
                    count = len(df[df['freshness_ts'] == ts])
                    print(f"  {ts}: {count} record(s)")
            
            # Show records sorted by freshness_ts (newest first)
            print(f"\n{'=' * 70}")
            print("Records by Freshness (Newest First)")
            print(f"{'=' * 70}")
            df_sorted = df.sort_values('freshness_ts', ascending=False)
            print(df_sorted[['custID', 'name', 'age', 'freshness_ts']].head(10).to_string(index=False))
        
        print("\n✓ Query completed successfully")
        return True
        
    except Exception as e:
        print(f"❌ Error querying dataset: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_filter_query():
    """Test filtering customer data"""
    
    # Load configuration
    config_manager = ConfigManager()
    config = config_manager.config
    
    # Construct dataset path
    base_path = config.lancedb.db_path
    dataset_path = f"{base_path}/{QUERY_TABLE}.lance"
    
    # Prepare storage options for S3
    storage_options = None
    if base_path.startswith('s3://'):
        storage_options = config.lancedb.storage_options or {}
        if config.lancedb.proxy:
            storage_options['proxy_options'] = config.lancedb.proxy
    
    try:
        dataset = lance.dataset(dataset_path, storage_options=storage_options)
        
        print("\n" + "=" * 70)
        print("Filtered Query Examples")
        print("=" * 70)
        
        # Example 1: Filter by freshness_ts (recent data)
        print("\n1. Most recent data (by freshness_ts):")
        scanner = dataset.scanner(limit=100)  # Get all data first
        table = scanner.to_table()
        df = table.to_pandas()
        
        # Sort by freshness_ts and show top 5
        df_recent = df.sort_values('freshness_ts', ascending=False).head(5)
        print(f"   Found {len(df_recent)} most recent records")
        print(df_recent[['custID', 'name', 'age', 'freshness_ts']].to_string(index=False))
        
        # Example 2: Group by freshness_ts
        print("\n2. Records grouped by freshness_ts:")
        ts_groups = df.groupby('freshness_ts').size().sort_index(ascending=False)
        print(f"   Total groups: {len(ts_groups)}")
        for ts, count in ts_groups.head(10).items():
            print(f"   {ts}: {count} record(s)")
        
        # Example 3: Filter by age
        print("\n3. Customers with age > 50:")
        scanner = dataset.scanner(filter="age > 50", limit=5)
        table = scanner.to_table()
        df_age = table.to_pandas()
        print(f"   Found {len(df_age)} records (showing first 5)")
        if len(df_age) > 0:
            print(df_age[['custID', 'name', 'age', 'freshness_ts']].to_string(index=False))
        
        return True
        
    except Exception as e:
        print(f"❌ Error in filtered query: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    print("Testing Lance Dataset Query")
    print()
    
    success = test_query_customer()
    
    if success:
        test_filter_query()
    
    sys.exit(0 if success else 1)
