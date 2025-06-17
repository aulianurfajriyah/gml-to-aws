#!/usr/bin/env python3
"""
Cesium ION Asset Status Checker

This script checks the upload and processing status of assets in Cesium ION.
"""

import sys
from cesium_helper import CesiumAPIHelper

def list_archives(cesium_helper: CesiumAPIHelper) -> None:
    """List available archives in Cesium ION."""
    print("📦 Listing available archives...")
    print("=" * 60)
    
    archives = cesium_helper.list_archived_assets()
    
    if not archives:
        print("❌ No archives found or error retrieving archives")
        return
    
    print("📦 Archived Assets:")
    print("-" * 40)
    for archive in archives:
        print(archive)
        


def main():    
    try:
        print("🌍 Cesium ION Archive Status Checker")
        print("=" * 50)
        
        # Initialize helper
        cesium_helper = CesiumAPIHelper()

        list_archives(cesium_helper)

    except ValueError as e:
        print(f"❌ Configuration Error: {e}")
        print("\n💡 Make sure to set your CESIUM_ION_TOKEN environment variable.")
        print("   You can create a .env file with: CESIUM_ION_TOKEN=your_token_here")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
