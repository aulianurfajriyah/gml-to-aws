#!/usr/bin/env python3
"""
Cesium ION Asset Status Checker

This script checks the upload and processing status of assets in Cesium ION.
"""

import sys
import argparse
from datetime import datetime
from typing import Optional, List
from cesium_helper import CesiumAPIHelper


def format_asset_info(asset: dict) -> str:
    """Format asset information for display."""
    asset_id = asset.get('id', 'Unknown')
    name = asset.get('name', 'Unnamed')
    status = asset.get('status', 'Unknown')
    asset_type = asset.get('type', 'Unknown')
    date_added = asset.get('dateAdded', 'Unknown')
    
    # Parse and format date if available
    if date_added != 'Unknown':
        try:
            parsed_date = datetime.fromisoformat(date_added.replace('Z', '+00:00'))
            date_added = parsed_date.strftime('%Y-%m-%d %H:%M:%S UTC')
        except:
            pass
    
    info = f"""
Asset ID: {asset_id}
Name: {name}
Type: {asset_type}
Status: {status}
Date Added: {date_added}
"""
    
    # Add processing info if available
    if 'percentComplete' in asset:
        percent = asset['percentComplete']
        info += f"Progress: {percent}%\n"
    
    if 'description' in asset:
        info += f"Description: {asset['description']}\n"
    
    return info.strip()


def get_status_emoji(status: str) -> str:
    """Get emoji representation for asset status."""
    status_emojis = {
        'COMPLETE': '✅',
        'ERROR': '❌',
        'PROCESSING': '⏳',
        'AWAITING_FILES': '📋',
        'NOT_STARTED': '⏸️',
        'DATA_ERROR': '🚨',
        'UPLOAD_COMPLETE': '📤'
    }
    return status_emojis.get(status.upper(), '❓')


def check_single_asset(cesium_helper: CesiumAPIHelper, asset_id: str) -> None:
    """Check status of a single asset."""
    print(f"🔍 Checking asset: {asset_id}")
    print("-" * 50)
    
    asset = cesium_helper.get_asset_status(asset_id)
    
    if asset is None:
        print(f"❌ Could not retrieve asset {asset_id}")
        print("   - Asset may not exist")
        print("   - Invalid asset ID")
        print("   - Authentication error")
        return
    
    status = asset.get('status', 'Unknown')
    emoji = get_status_emoji(status)
    
    print(f"{emoji} Status: {status}")
    print(format_asset_info(asset))


def check_multiple_assets(cesium_helper: CesiumAPIHelper, asset_ids: List[str]) -> None:
    """Check status of multiple assets."""
    print(f"🔍 Checking {len(asset_ids)} assets...")
    print("=" * 60)
    
    for i, asset_id in enumerate(asset_ids, 1):
        print(f"\n[{i}/{len(asset_ids)}] Asset: {asset_id}")
        print("-" * 40)
        
        asset = cesium_helper.get_asset_status(asset_id)
        
        if asset is None:
            print(f"❌ Could not retrieve asset {asset_id}")
            continue
        
        status = asset.get('status', 'Unknown')
        emoji = get_status_emoji(status)
        name = asset.get('name', 'Unnamed')
        
        print(f"{emoji} {name} - {status}")
        
        if 'percentComplete' in asset:
            percent = asset['percentComplete']
            print(f"📊 Progress: {percent}%")


def list_recent_assets(cesium_helper: CesiumAPIHelper, limit: int = 10) -> None:
    """List recent assets with their statuses."""
    print(f"📋 Recent Assets (limit: {limit})")
    print("=" * 60)
    
    assets = cesium_helper.get_cesium_ion_assets_list()
    
    if not assets:
        print("❌ No assets found or error retrieving assets")
        return
    
    # Sort by date added (most recent first) and limit
    try:
        assets_sorted = sorted(
            assets, 
            key=lambda x: x.get('dateAdded', ''), 
            reverse=True
        )[:limit]
    except:
        assets_sorted = assets[:limit]
    
    for i, asset in enumerate(assets_sorted, 1):
        asset_id = asset.get('id', 'Unknown')
        name = asset.get('name', 'Unnamed')
        status = asset.get('status', 'Unknown')
        emoji = get_status_emoji(status)
        
        print(f"{i:2d}. {emoji} [{asset_id}] {name} - {status}")


def monitor_assets(cesium_helper: CesiumAPIHelper, asset_ids: List[str], interval: int = 30) -> None:
    """Monitor assets with periodic status updates."""
    import time
    
    print(f"👀 Monitoring {len(asset_ids)} assets (checking every {interval}s)")
    print("Press Ctrl+C to stop monitoring")
    print("=" * 60)
    
    try:
        while True:
            print(f"\n🔄 Status check at {datetime.now().strftime('%H:%M:%S')}")
            print("-" * 40)
            
            all_complete = True
            
            for asset_id in asset_ids:
                asset = cesium_helper.get_asset_status(asset_id)
                
                if asset is None:
                    print(f"❌ {asset_id}: Could not retrieve")
                    continue
                
                status = asset.get('status', 'Unknown')
                emoji = get_status_emoji(status)
                name = asset.get('name', 'Unnamed')
                
                if status.upper() not in ['COMPLETE', 'ERROR', 'DATA_ERROR']:
                    all_complete = False
                
                progress_info = ""
                if 'percentComplete' in asset:
                    progress_info = f" ({asset['percentComplete']}%)"
                
                print(f"{emoji} {name}: {status}{progress_info}")
            
            if all_complete:
                print("\n🎉 All monitored assets have completed processing!")
                break
            
            print(f"\n⏰ Next check in {interval} seconds...")
            time.sleep(interval)
            
    except KeyboardInterrupt:
        print("\n\n👋 Monitoring stopped by user")

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
        archive_id = archive.get('id', 'Unknown')
        name = archive.get('name', 'Unnamed')
        status = archive.get('status', 'Unknown')
        download_url = archive.get('downloadUrl')

        print(f"  • Archive ID: {archive_id}")
        print(f"    Name: {name}")
        print(f"    Status: {status}")
        if download_url:
            print(f"    Download: {download_url}")
        print(f"    View Archive: https://ion.cesium.com/archives/{archive_id}")
        


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Check Cesium ION asset upload and processing status",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python check_status.py 12345                    # Check single asset
  python check_status.py 12345 67890              # Check multiple assets
  python check_status.py --list                   # List recent assets
  python check_status.py --list --limit 20        # List 20 recent assets
  python check_status.py --monitor 12345 67890    # Monitor assets
        """
    )
    
    parser.add_argument(
        'asset_ids', 
        nargs='*', 
        help='Asset ID(s) to check'
    )
    
    parser.add_argument(
        '--list', 
        action='store_true', 
        help='List recent assets'
    )
    
    parser.add_argument(
        '--limit', 
        type=int, 
        default=10, 
        help='Limit number of assets to list (default: 10)'
    )
    
    parser.add_argument(
        '--monitor', 
        action='store_true', 
        help='Monitor assets with periodic updates'
    )
    
    parser.add_argument(
        '--interval', 
        type=int, 
        default=30, 
        help='Monitoring interval in seconds (default: 30)'
    )
    
    args = parser.parse_args()
    
    try:
        print("🌍 Cesium ION Asset Status Checker")
        print("=" * 50)
        
        # Initialize helper
        cesium_helper = CesiumAPIHelper()
        
        if args.list:
            list_recent_assets(cesium_helper, args.limit)
        
        elif args.asset_ids:
            if args.monitor:
                monitor_assets(cesium_helper, args.asset_ids, args.interval)
            elif len(args.asset_ids) == 1:
                check_single_asset(cesium_helper, args.asset_ids[0])
            else:
                check_multiple_assets(cesium_helper, args.asset_ids)
        
        else:
            print("❌ No asset IDs provided and --list not specified")
            print("\nUsage examples:")
            print("  python check_status.py 12345              # Check single asset")
            print("  python check_status.py --list             # List recent assets")
            print("  python check_status.py --help             # Show all options")
        
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
