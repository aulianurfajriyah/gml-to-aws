#!/usr/bin/env python3
"""
GML to Cesium ION Uploader

This script uploads GML files from the data folder to Cesium ION API
with parallel processing and progress tracking.
"""

from pathlib import Path
from cesium_helper import CesiumAPIHelper


def main():
    """Main function to orchestrate the upload process."""
    try:
        print("GML to Cesium ION Uploader")
        print("=" * 50)
        
        # Initialize uploader
        cesiumHelper = CesiumAPIHelper()
        
        # Get GML files
        gml_files = cesiumHelper.get_gml_files()
        
        if not gml_files:
            print("❌ No GML files found in the 'data' folder.")
            return
        
        print(f"📁 Found {len(gml_files)} GML files")
        
        # Upload files in parallel
        cesiumHelper.upload_files_parallel(gml_files)
        
        # Print summary
        cesiumHelper.print_summary()
        
    except ValueError as e:
        print(f"❌ Configuration Error: {e}")
        print("\n💡 Make sure to set your CESIUM_ION_TOKEN environment variable.")
        print("   You can create a .env file with: CESIUM_ION_TOKEN=your_token_here")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")


if __name__ == "__main__":
    main()
