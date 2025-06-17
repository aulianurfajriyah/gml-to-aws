#!/usr/bin/env python3
"""
GML to Cesium ION Uploader

This script uploads GML files from the data folder to Cesium ION API
"""

import argparse
from pathlib import Path
from cesium_helper import CesiumAPIHelper


def main():
    """Main function to orchestrate the upload process."""
    parser = argparse.ArgumentParser(
        description="Upload GML files to Cesium ION with complete workflow",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                     # Upload files, don't wait for processing
  python main.py --wait              # Upload files and wait for processing
  python main.py --workers 5         # Use 5 concurrent uploads
        """
    )
    
    parser.add_argument(
        '--wait', 
        action='store_true', 
        help='Wait for processing completion (can take several minutes)'
    )
    
    parser.add_argument(
        '--workers', 
        type=int, 
        default=5, 
        help='Number of concurrent uploads (default: 5)'
    )
    
    args = parser.parse_args()
    
    try:
        print("🌍 GML to Cesium ION Uploader")
        print("=" * 50)
        
        # Initialize uploader
        cesiumHelper = CesiumAPIHelper()
        
        # Get GML files
        gml_files = cesiumHelper.get_gml_files()
        
        if not gml_files:
            print("❌ No GML files found in the 'data' folder.")
            return
        
        print(f"📁 Found {len(gml_files)} GML files")
        if args.wait:
            print("⏳ Will monitor processing status until completion")
        
        # Upload files in parallel
        cesiumHelper.upload_files_parallel(
            gml_files, 
            max_workers=args.workers, 
            wait_for_completion=args.wait
        )
        
        # Print summary
        cesiumHelper.print_summary()
        
        # If uploads were successful and we didn't wait, show monitoring tip
        if not args.wait and cesiumHelper.results['success']:
            asset_ids = cesiumHelper.get_asset_ids_from_results()
            if asset_ids:
                print("\n💡 Monitor processing status with:")
                print(f"   python check_status.py {' '.join(asset_ids[:3])}")
                if len(asset_ids) > 3:
                    print(f"   (showing first 3 of {len(asset_ids)} assets)")
        
    except ValueError as e:
        print(f"❌ Configuration Error: {e}")
        print("\n💡 Make sure to set your CESIUM_ION_TOKEN environment variable.")
        print("   You can create a .env file with: CESIUM_ION_TOKEN=your_token_here")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")


if __name__ == "__main__":
    main()
