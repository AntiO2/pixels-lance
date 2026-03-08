#!/usr/bin/env python3
"""
Create composite BTREE indexes for all tables based on their primary keys from schema YAML files.

Usage:
    python3 scripts/ensure_index.py [--schema-type chbenchmark|hybench] [--tables table1 table2 ...] [--temp-dir /path/to/tmp]

Examples:
    # Create indexes for all chbenchmark tables
    python3 scripts/ensure_index.py --schema-type chbenchmark
    
    # Create indexes for specific tables with custom temp directory
    python3 scripts/ensure_index.py --schema-type hybench --tables customer stock --temp-dir /mnt/data/tmp
    
    # Create indexes with plenty of temp space to handle large datasets
    python3 scripts/ensure_index.py --temp-dir /mnt/fast_ssd/tmp

Note:
    If you encounter "No space left on device" errors, use --temp-dir to specify a directory
    with more available space. This directory will be used by Lance for intermediate spillover
    during index creation on large tables.
"""

import argparse
import os
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import yaml
import lance
from pixels_lance.config import ConfigManager


def load_schema_file(schema_path):
    """Load schema YAML file and extract table definitions"""
    with open(schema_path, 'r') as f:
        schema_data = yaml.safe_load(f)
    
    tables = schema_data.get('tables', {})
    return tables


def ensure_composite_index(ds, table_name, pk_columns):
    """
    确保主键复合索引存在，不存在则创建。
    
    Lance的索引创建会使用TMPDIR环境变量来处理大型数据的spillover。
    如果遇到"No space left on device"错误，请使用--tmp-dir参数指定有更多空间的目录。
    """
    if not pk_columns:
        print(f"  ⚠ 表 {table_name} 没有定义主键，跳过索引创建")
        return
    
    # Ensure pk_columns is a list
    if isinstance(pk_columns, str):
        pk_columns = [pk_columns]
    
    try:
        existing_indices = ds.list_indices()
        print(f"  现有索引数量: {len(existing_indices)}")
        
        # Check if composite index on all PK columns exists
        composite_exists = False
        for idx in existing_indices:
            idx_fields = idx.get('fields', [])
            # Check if index covers all our PK columns
            if set(idx_fields) == set(pk_columns):
                composite_exists = True
                print(f"    ✓ 复合索引已存在: {idx_fields}")
                break
        
        if not composite_exists:
            print(f"  创建复合索引: {pk_columns} (BTREE)", flush=True)
            print(f"    使用TMPDIR: {os.environ.get('TMPDIR', '/tmp')}", flush=True)
            try:
                # Create composite BTREE index on all primary key columns
                # For upsert operations, this is more efficient than separate single-column indexes
                ds.create_scalar_index(pk_columns, index_type="BTREE")
                print(f"    ✓ 复合索引创建完成: {pk_columns}", flush=True)
            except Exception as e:
                print(f"    ✗ 复合索引创建失败: {e}", flush=True)
                # Fallback: try creating individual indexes
                if len(pk_columns) > 1:
                    print(f"  回退: 尝试创建单列索引...")
                    for col in pk_columns:
                        try:
                            ds.create_scalar_index(col, index_type="BTREE")
                            print(f"      ✓ 单列索引创建完成: {col}", flush=True)
                        except Exception as e2:
                            print(f"      ✗ 单列索引创建失败: {col} - {e2}", flush=True)
    except Exception as e:
        print(f"  警告: 索引操作失败: {e}")


def process_table(table_name, pk_columns, base_path, storage_options):
    """Process a single table: load dataset and ensure index"""
    dataset_path = f"{base_path.rstrip('/')}/{table_name}.lance"
    
    print(f"\n{'=' * 70}")
    print(f"表: {table_name}")
    print(f"主键: {pk_columns}")
    print(f"路径: {dataset_path}")
    print(f"{'=' * 70}")
    
    # Load dataset
    try:
        if storage_options:
            ds = lance.dataset(dataset_path, storage_options=storage_options)
        else:
            ds = lance.dataset(dataset_path)
        
        print(f"  ✓ 数据集加载成功")
        print(f"    记录数: {ds.count_rows()}")
        
        # Create composite index
        ensure_composite_index(ds, table_name, pk_columns)
        
    except FileNotFoundError:
        print(f"  ⚠ 数据集不存在，跳过: {dataset_path}")
    except Exception as e:
        print(f"  ✗ 错误: {e}")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Create indexes for all tables in schema")
    parser.add_argument(
        "--schema-type",
        choices=["chbenchmark", "hybench"],
        default="chbenchmark",
        help="Schema type (default: chbenchmark)",
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        help="Specific tables to process (default: all tables in schema)",
    )
    parser.add_argument(
        "--temp-dir",
        help="Temporary directory for Lance/Arrow spillover files (e.g., /mnt/fast_ssd/tmp). "
             "Use this if you encounter 'No space left on device' errors.",
    )
    args = parser.parse_args()

    if args.temp_dir:
        tmp_path = Path(args.temp_dir).expanduser().resolve()
        tmp_path.mkdir(parents=True, exist_ok=True)
        os.environ["TMPDIR"] = str(tmp_path)
        os.environ["TMP"] = str(tmp_path)
        os.environ["TEMP"] = str(tmp_path)
        print(f"设置临时目录: {os.environ['TMPDIR']}\n")
    
    # Determine schema file path
    project_root = Path(__file__).parent.parent
    schema_file = project_root / "config" / f"schema_{args.schema_type}.yaml"
    
    if not schema_file.exists():
        print(f"✗ 错误: Schema 文件不存在: {schema_file}")
        sys.exit(1)
    
    # Load config to get dataset path
    try:
        config_mgr = ConfigManager()
        config = config_mgr.get()
        base_path = config.lancedb.db_path
        storage_options = config.lancedb.storage_options or {}
        
        # Filter out None values
        storage_options = {k: v for k, v in storage_options.items() if v is not None and v != ''}
    except Exception as e:
        print(f"错误: 无法加载配置: {e}")
        sys.exit(1)
    
    # Load schema
    print("=" * 70)
    print(f"批量索引创建工具")
    print("=" * 70)
    print(f"Schema 类型: {args.schema_type}")
    print(f"Schema 文件: {schema_file}")
    print(f"数据集根路径: {base_path}")
    if args.temp_dir:
        print(f"临时目录: {Path(args.temp_dir).expanduser().resolve()}")
    else:
        print(f"临时目录: {os.environ.get('TMPDIR', '/tmp')} (系统默认)")
    print(f"存储选项: {list(storage_options.keys()) if storage_options else 'None (local)'}")
    print("=" * 70)
    
    tables = load_schema_file(schema_file)
    
    # Filter tables if specified
    if args.tables:
        tables = {name: spec for name, spec in tables.items() if name in args.tables}
        if not tables:
            print(f"✗ 错误: 未找到指定的表")
            sys.exit(1)
    
    print(f"\n将为 {len(tables)} 个表创建索引...\n")
    
    success_count = 0
    skip_count = 0
    error_count = 0
    
    # Process each table
    for table_name, table_spec in tables.items():
        pk = table_spec.get('primary_key') or table_spec.get('pk')
        
        try:
            process_table(table_name, pk, base_path, storage_options)
            success_count += 1
        except FileNotFoundError:
            skip_count += 1
        except Exception:
            error_count += 1
    
    # Summary
    print(f"\n{'=' * 70}")
    print("索引创建完成！")
    print("=" * 70)
    print(f"总表数: {len(tables)}")
    print(f"成功: {success_count}")
    print(f"跳过 (数据集不存在): {skip_count}")
    print(f"错误: {error_count}")
    print("=" * 70)

if __name__ == "__main__":
    main()
