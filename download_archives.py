#!/usr/bin/env python3
"""
Cesium ION Archive Downloader

This script downloads archives from Cesium ION and saves them to the 'converted' folder.
"""

import sys
import argparse
from pathlib import Path
from cesium_helper import CesiumAPIHelper


def main():
    """Main function to download archives."""
    parser = argparse.ArgumentParser(
        description="Download archives from Cesium ION",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python download_archives.py                    # Download all completed archives
  python download_archives.py --archive-ids 123 456  # Download specific archives
  python download_archives.py --output-dir downloads  # Download to custom directory
  python download_archives.py --logging          # Enable detailed logging
        """
    )
    
    parser.add_argument(
        '--archive-ids',
        nargs='*',
        help='Specific archive IDs to download (if not provided, downloads all completed archives)'
    )
    
    parser.add_argument(
        '--output-dir',
        default='converted',
        help='Output directory for downloaded archives (default: converted)'
    )
    
    parser.add_argument(
        '--logging',
        action='store_true',
        help='Enable detailed logging (default: false)'
    )
    
    parser.add_argument(
        '--list-only',
        action='store_true',
        help='List available archives without downloading'
    )

    args = parser.parse_args()
    
    try:
        print("📥 Cesium ION Archive Downloader")
        print("=" * 50)
        
        # Initialize helper - this will validate the API token
        try:
            cesium_helper = CesiumAPIHelper(enable_logging=args.logging)
        except ValueError as e:
            if "CESIUM_ION_TOKEN" in str(e):
                print(f"❌ Configuration Error: {e}")
                print("\n💡 Make sure to set your CESIUM_ION_TOKEN environment variable.")
                print("   You can create a .env file with: CESIUM_ION_TOKEN=your_token_here")
                sys.exit(1)
            else:
                raise
        
        if args.list_only:
            # Just list available archives
            archived_assets = cesium_helper.list_archived_assets()
            
            if not archived_assets:
                print("❌ No archives found")
                return
            
            print(f"📦 Found {len(archived_assets)} archives:")
            print("-" * 60)
            
            for archive in archived_assets:
                archive_id = archive.get('id', 'Unknown')
                name = archive.get('name', 'Unnamed')
                status = archive.get('status', 'Unknown')
                size = archive.get('size', 0)
                asset_ids = archive.get('assetIds', [])
                
                status_emoji = "✅" if status == "COMPLETE" else "⏳" if status == "PROCESSING" else "❌"
                
                print(f"  {status_emoji} Archive ID: {archive_id}")
                print(f"    Name: {name}")
                print(f"    Status: {status}")
                print(f"    Size: {size:.2f} MB")
                print(f"    Asset IDs: {', '.join(map(str, asset_ids))}")
                print()
            
            return
        
        if args.archive_ids:
            # Download specific archives
            print(f"📥 Downloading {len(args.archive_ids)} specific archives...")
            
            download_results = []
            for archive_id in args.archive_ids:
                print(f"\n🔽 Downloading archive {archive_id}...")
                success, file_path = cesium_helper.download_archive(archive_id, args.output_dir)
                
                result = {
                    'archive_id': archive_id,
                    'success': success,
                    'file_path': file_path
                }
                download_results.append(result)
                
                if success:
                    print(f"✅ Archive {archive_id} downloaded successfully to: {file_path}")
                else:
                    print(f"❌ Failed to download archive {archive_id}")
            
            # Print summary
            successful = len([r for r in download_results if r['success']])
            failed = len([r for r in download_results if not r['success']])
            
            print(f"\n📊 Download Summary: {successful}/{len(args.archive_ids)} archives downloaded successfully")
            
            if successful > 0:
                print("\n✅ SUCCESSFUL DOWNLOADS:")
                for result in download_results:
                    if result['success']:
                        print(f"  • Archive {result['archive_id']}: {result['file_path']}")
            
            if failed > 0:
                print("\n❌ FAILED DOWNLOADS:")
                for result in download_results:
                    if not result['success']:
                        print(f"  • Archive {result['archive_id']}")
        
        else:
            # Download all completed archives
            print("📥 Downloading all completed archives...")
            download_results = cesium_helper.download_all_completed_archives(args.output_dir)
            
            if not download_results:
                print("❌ No archives were downloaded")
                return
            
            # Results are already printed by the method
            print(f"\n📂 All downloaded files are saved in: {Path(args.output_dir).absolute()}")
        
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
