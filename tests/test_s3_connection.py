#!/usr/bin/env python3
"""
测试 S3 配置和连接
"""
import sys
import pyarrow as pa
import lance
from pathlib import Path

# 添加 src 到 path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pixels_lance.config import ConfigManager

def test_config_loading():
    """测试配置加载"""
    print("=" * 60)
    print("1. 测试配置加载")
    print("=" * 60)
    
    config_manager = ConfigManager()
    config = config_manager.config
    print(f"✓ 配置加载成功")
    print(f"  - db_path: {config.lancedb.db_path}")
    print(f"  - table_name: {config.lancedb.table_name}")
    print(f"  - mode: {config.lancedb.mode}")
    
    if config.lancedb.storage_options:
        print(f"  - storage_options:")
        for key, value in config.lancedb.storage_options.items():
            # 隐藏敏感信息
            if 'secret' in key.lower() or 'key' in key.lower():
                display_value = value[:8] + "..." if value and len(value) > 8 else "***"
            else:
                display_value = value
            print(f"    - {key}: {display_value}")
    else:
        print("  - storage_options: {} (本地存储)")
    print()
    
    return config_manager

def test_s3_write_read(config_manager):
    """测试 S3 写入和读取"""
    print("=" * 60)
    print("2. 测试 S3 写入和读取")
    print("=" * 60)
    
    config = config_manager.config
    
    # 准备测试数据
    test_data = pa.table({
        'id': [1, 2, 3],
        'name': ['test1', 'test2', 'test3'],
        'value': [100, 200, 300]
    })
    
    # 构建测试路径
    base_path = config.lancedb.db_path.rstrip('/')
    test_path = f"{base_path}/test_connection.lance"
    
    # 过滤 storage_options
    storage_options = {}
    if config.lancedb.storage_options:
        storage_options = {k: v for k, v in config.lancedb.storage_options.items() 
                          if v is not None and v != ''}
    
    print(f"测试路径: {test_path}")
    print(f"Storage options: {list(storage_options.keys())}")
    print()
    
    try:
        # 测试写入
        print("尝试写入数据...")
        lance.write_dataset(test_data, test_path, storage_options=storage_options)
        print("✓ 写入成功")
        print()
        
        # 测试读取
        print("尝试读取数据...")
        dataset = lance.dataset(test_path, storage_options=storage_options)
        print("✓ 读取成功")
        print(f"  - 数据行数: {dataset.count_rows()}")
        print(f"  - Schema: {dataset.schema}")
        print()
        
        # 读取前 3 行
        print("读取数据内容:")
        data = dataset.to_table()
        print(data.to_pandas())
        print()
        
        # 清理测试数据
        print("清理测试数据...")
        try:
            dataset.delete("id > 0")
            print("✓ 删除成功")
        except Exception as e:
            print(f"⚠ 删除失败（可忽略）: {e}")
        print()
        
        return True
        
    except Exception as e:
        print(f"✗ 失败: {type(e).__name__}")
        print(f"  错误信息: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def main():
    print("\n")
    print("╔" + "=" * 58 + "╗")
    print("║" + " " * 15 + "S3 配置测试工具" + " " * 28 + "║")
    print("╚" + "=" * 58 + "╝")
    print()
    
    # 测试 1: 配置加载
    try:
        config_manager = test_config_loading()
    except Exception as e:
        print(f"✗ 配置加载失败: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    # 测试 2: S3 读写
    success = test_s3_write_read(config_manager)
    
    print("=" * 60)
    if success:
        print("✓ 所有测试通过！S3 配置正确")
    else:
        print("✗ S3 连接测试失败，请检查配置")
    print("=" * 60)
    print()
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())
