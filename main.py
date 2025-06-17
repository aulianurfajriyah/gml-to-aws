#!/usr/bin/env python3
"""
GML to Cesium ION Uploader

This script uploads GML files from the data folder to Cesium ION API
"""

import argparse
import logging
from pathlib import Path
from datetime import datetime
from cesium_helper import CesiumAPIHelper


def setup_main_logging(enabled: bool = False) -> logging.Logger:
    """Set up logging for the main script.
    
    Args:
        enabled: Whether to enable logging (default: False)
    """
    logger = logging.getLogger('main')
    
    if not enabled:
        # Set logger to CRITICAL level to disable all logging
        logger.setLevel(logging.CRITICAL + 1)
        # Remove any existing handlers
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
    else:
        logger.setLevel(logging.INFO)
    
    return logger


def main():
    """Main function to orchestrate the upload process."""
    parser = argparse.ArgumentParser(
        description="Upload GML files to Cesium ION with complete workflow",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                        # Upload files, don't wait for processing
  python main.py --wait                 # Upload files and wait for processing
  python main.py --wait --archive       # Upload, wait for processing, and create archives
  python main.py --wait --archive --download  # Upload, wait, create archives, and download them
  python main.py --workers 5            # Use 5 concurrent uploads
  python main.py --logging              # Enable detailed logging
  python main.py --wait --logging       # Upload, wait, and enable logging
  python main.py --wait --archive --logging  # Full workflow with logging
  python main.py --wait --archive --download --logging  # Complete workflow with downloads and logging
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
    
    parser.add_argument(
        '--logging', 
        action='store_true', 
        help='Enable detailed logging to file and console (default: false)'
    )
    
    parser.add_argument(
        '--archive', 
        action='store_true', 
        help='Create archives after successful processing (requires --wait)'
    )
    
    parser.add_argument(
        '--download', 
        action='store_true', 
        help='Download archives after creation to converted folder (requires --archive)'
    )

    args = parser.parse_args()
    
    # Validate arguments
    if args.archive and not args.wait:
        print("❌ Error: --archive requires --wait (archives can only be created after processing completes)")
        parser.print_help()
        return
    
    if args.download and not args.archive:
        print("❌ Error: --download requires --archive (downloads can only happen after archive creation)")
        parser.print_help()
        return
    
    # Set up logging for main script
    logger = setup_main_logging(args.logging)
    
    def log_if_enabled(level: str, message: str):
        """Helper function for conditional logging in main."""
        if args.logging:
            getattr(logger, level)(message)
    
    try:
        print("🌍 GML to Cesium ION Uploader")
        print("=" * 50)
        
        log_if_enabled("info", "=== GML to Cesium ION Uploader Started ===")
        log_if_enabled("info", f"Command line arguments: wait={args.wait}, workers={args.workers}, logging={args.logging}, archive={args.archive}, download={args.download}")
        
        # Initialize uploader (this will set up the main logging)
        cesiumHelper = CesiumAPIHelper(enable_logging=args.logging)
        
        # Get GML files
        gml_files = cesiumHelper.get_gml_files()
        
        if not gml_files:
            log_if_enabled("warning", "No GML files found in the 'data' folder")
            print("❌ No GML files found in the 'data' folder.")
            return
        
        print(f"📁 Found {len(gml_files)} GML files")
        if args.wait:
            print("⏳ Will monitor processing status until completion")
        if args.archive:
            print("📦 Will create archives after processing completion")
        if args.download:
            print("📥 Will download archives to 'converted' folder")
        
        log_if_enabled("info", f"Starting upload process for {len(gml_files)} files")
        
        # Upload files in parallel
        start_time = datetime.now()
        cesiumHelper.upload_files_parallel(
            gml_files, 
            max_workers=args.workers, 
            wait_for_completion=args.wait,
            create_archive=args.archive,
            download_archive=args.download
        )
        end_time = datetime.now()
        
        # Log timing information
        duration = end_time - start_time
        log_if_enabled("info", f"Upload process completed in {duration.total_seconds():.2f} seconds")
        
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
                
                log_if_enabled("info", f"Generated monitoring command for {len(asset_ids)} assets")
        
        # Final success/failure determination
        success_count = len(cesiumHelper.results['success'])
        archived_count = len(cesiumHelper.results['archived'])
        downloaded_count = len([item for item in cesiumHelper.results['archived'] if item.get('download_path')])
        total_count = len(gml_files)
        
        if success_count == total_count:
            log_if_enabled("info", "✅ All uploads completed successfully")
            if args.download and downloaded_count > 0:
                print(f"\n🎉 All uploads completed successfully! {archived_count} archives created and {downloaded_count} downloaded!")
            elif args.archive and archived_count > 0:
                print(f"\n🎉 All uploads completed successfully! {archived_count} archives created!")
            else:
                print("\n🎉 All uploads completed successfully!")
        elif success_count > 0:
            log_if_enabled("warning", f"⚠️ Partial success: {success_count}/{total_count} uploads succeeded")
            if args.download and downloaded_count > 0:
                print(f"\n⚠️ Partial success: {success_count}/{total_count} uploads succeeded, {archived_count} archives created, {downloaded_count} downloaded")
            elif args.archive and archived_count > 0:
                print(f"\n⚠️ Partial success: {success_count}/{total_count} uploads succeeded, {archived_count} archives created")
            else:
                print(f"\n⚠️ Partial success: {success_count}/{total_count} uploads succeeded")
        else:
            log_if_enabled("error", "❌ All uploads failed")
            print("\n❌ All uploads failed")
        
    except ValueError as e:
        log_if_enabled("error", f"Configuration Error: {e}")
        print(f"❌ Configuration Error: {e}")
        print("\n💡 Make sure to set your CESIUM_ION_TOKEN environment variable.")
        print("   You can create a .env file with: CESIUM_ION_TOKEN=your_token_here")
    except Exception as e:
        log_if_enabled("error", f"Unexpected error: {e}")
        print(f"❌ Unexpected error: {e}")
    finally:
        log_if_enabled("info", "=== GML to Cesium ION Uploader Finished ===")


if __name__ == "__main__":
    main()
