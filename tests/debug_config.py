#!/usr/bin/env python3
"""
诊断配置和代理设置
"""
import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pixels_lance.config import ConfigManager
from pixels_lance.storage import LanceDBStore

print("=" * 60)
print("环境变量检查")
print("=" * 60)
print(f"HTTP_PROXY: {os.getenv('HTTP_PROXY', '(未设置)')}")
print(f"HTTPS_PROXY: {os.getenv('HTTPS_PROXY', '(未设置)')}")
print(f"AWS_REGION: {os.getenv('AWS_REGION', '(未设置)')}")
print(f"AWS_ACCESS_KEY_ID: {os.getenv('AWS_ACCESS_KEY_ID', '(未设置)')[:8]}..." if os.getenv('AWS_ACCESS_KEY_ID') else "(未设置)")
print()

print("=" * 60)
print("配置文件加载")
print("=" * 60)
config_manager = ConfigManager()
config = config_manager.config

print(f"db_path: {config.lancedb.db_path}")
print(f"proxy: {getattr(config.lancedb, 'proxy', '(未配置)')}")
print()

if config.lancedb.storage_options:
    print("storage_options:")
    for k, v in config.lancedb.storage_options.items():
        if 'secret' in k.lower() or 'key' in k.lower():
            print(f"  {k}: {v[:8]}..." if v else f"  {k}: (空)")
        else:
            print(f"  {k}: {v}")
print()

print("=" * 60)
print("LanceDBStore 初始化")
print("=" * 60)
store = LanceDBStore()

print(f"Base path: {store.base_path}")
print(f"Final storage_options:")
for k, v in store.storage_options.items():
    if 'secret' in k.lower() or 'key' in k.lower():
        print(f"  {k}: {v[:8]}..." if v else f"  {k}: (空)")
    elif k == 'proxy_options':
        print(f"  {k}: {v} ⭐")
    else:
        print(f"  {k}: {v}")
print()

if 'proxy_options' in store.storage_options:
    print("✓ 代理已配置")
else:
    print("✗ 代理未配置")
    print("\n请检查:")
    print("1. config/.env 中是否设置了 HTTP_PROXY")
    print("2. config/config.yaml 中是否有 proxy: ${HTTP_PROXY:-}")
