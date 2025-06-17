#!/usr/bin/env python3
"""
Test script to validate the GML uploader setup
"""

import os
import sys
from pathlib import Path

def test_dependencies():
    """Test if all required dependencies are available."""
    print("🧪 Testing Dependencies...")
    
    try:
        import requests
        print("  ✅ requests - OK")
    except ImportError:
        print("  ❌ requests - MISSING")
        return False
    
    try:
        import tqdm
        print("  ✅ tqdm - OK")
    except ImportError:
        print("  ❌ tqdm - MISSING")
        return False
    
    try:
        import dotenv
        print("  ✅ python-dotenv - OK")
    except ImportError:
        print("  ❌ python-dotenv - MISSING")
        return False
    
    return True

def test_files():
    """Test if required files and folders exist."""
    print("\n📁 Testing Files and Folders...")
    
    required_files = [
        'main.py',
        'requirements.txt',
        'setup.sh',
        '.env',
        '.env.example',
        'README.md'
    ]
    
    for file in required_files:
        if Path(file).exists():
            print(f"  ✅ {file} - EXISTS")
        else:
            print(f"  ❌ {file} - MISSING")
            return False
    
    # Test data folder
    data_folder = Path('data')
    if data_folder.exists() and data_folder.is_dir():
        gml_files = list(data_folder.glob('*.gml'))
        print(f"  ✅ data/ - EXISTS ({len(gml_files)} GML files)")
        for gml_file in gml_files:
            print(f"    • {gml_file.name}")
    else:
        print("  ❌ data/ - MISSING")
        return False
    
    return True

def test_environment():
    """Test environment configuration."""
    print("\n🔧 Testing Environment...")
    
    # Test virtual environment
    if hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix):
        print("  ✅ Virtual environment - ACTIVE")
    else:
        print("  ⚠️  Virtual environment - NOT ACTIVE")
        print("     Run: source venv/bin/activate")
    
    # Test .env file
    from dotenv import load_dotenv
    load_dotenv()
    
    token = os.getenv('CESIUM_ION_TOKEN')
    if token and token != 'your_cesium_ion_token_here':
        print("  ✅ CESIUM_ION_TOKEN - SET")
    else:
        print("  ⚠️  CESIUM_ION_TOKEN - NOT SET")
        print("     Edit .env file and set your token")
    
    return True

def main():
    """Run all tests."""
    print("🔬 GML Cesium Uploader - Setup Validation")
    print("=" * 50)
    
    deps_ok = test_dependencies()
    files_ok = test_files()
    env_ok = test_environment()
    
    print("\n" + "=" * 50)
    if deps_ok and files_ok:
        print("🎉 Setup validation completed successfully!")
        print("\n📝 Next steps:")
        print("1. Set your Cesium ION token in .env file")
        print("2. Run: python main.py")
    else:
        print("❌ Setup validation failed!")
        print("\n🔧 Fix the issues above and run this test again")

if __name__ == "__main__":
    main()
