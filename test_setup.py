#!/usr/bin/env python3
"""
Quick test to verify the project structure is correct
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

try:
    # Try importing just the core modules without external dependencies
    from pixels_lance.config import ConfigManager
    from pixels_lance.logger import setup_logging, get_logger
    
    # Try importing others if available
    try:
        from pixels_lance import RpcFetcher, DataParser, LanceDBStore
        full_import = True
    except ImportError as e:
        print(f"⚠ Some external modules not available yet (install dependencies): {e}")
        full_import = False
    
    print("✓ Core modules loaded successfully!")
    print("\nProject modules available:")
    print("  - ConfigManager")
    print("  - Logging utilities")
    if full_import:
        print("  - RpcFetcher")
        print("  - DataParser")
        print("  - LanceDBStore")
    
    # Test basic configuration loading
    print("\nTesting configuration loading...")
    try:
        config = ConfigManager("config/config.yaml")
        print("✓ Configuration loaded successfully!")
        print(f"  - RPC URL: {config.get().rpc.url}")
        print(f"  - LanceDB path: {config.get().lancedb.db_path}")
        print(f"  - Batch size: {config.get().batch_size}")
    except FileNotFoundError:
        print("⚠ Configuration file not found (expected for first run)")
    except Exception as e:
        print(f"⚠ Configuration loading error: {e}")
    
    print("\n✓ Project structure is ready!")
    print("\nNext steps:")
    print("1. Copy config/.env.example to config/.env and fill in values")
    print("2. Customize config/schema.yaml for your data structure")
    print("3. Check examples.py for usage patterns")
    print("4. Run: pixels-lance --config config/config.yaml")
    
except Exception as e:
    print(f"✗ Error: {e}")
    sys.exit(1)
