#!/usr/bin/env python3
"""
Cesium ION API Helper

This module provides helper functions for interacting with Cesium ION API.
Implements the complete 4-step upload workflow:
1. Create asset metadata
2. Upload files to S3
3. Notify completion
4. Monitor status
5. Create and download archives
"""

import os
import glob
import json
import time
import boto3
import requests
import logging
import shutil
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Tuple, Optional
from tqdm import tqdm
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


def setup_logging(enabled: bool = False) -> logging.Logger:
    """Set up logging configuration for the upload process.
    
    Args:
        enabled: Whether to enable file and console logging (default: False)
    """
    logger = logging.getLogger(__name__)
    
    if not enabled:
        # Set logger to CRITICAL level to disable all logging
        logger.setLevel(logging.CRITICAL + 1)
        # Remove any existing handlers
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        return logger
    
    # Create logs directory if it doesn't exist
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    
    # Create timestamp for log file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = logs_dir / f"cesium_upload_{timestamp}.log"
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()  # Also log to console
        ]
    )
    
    logger.setLevel(logging.INFO)
    logger.info(f"=== Cesium ION Upload Session Started ===")
    logger.info(f"Log file: {log_file}")
    
    return logger


class CesiumAPIHelper:
    def __init__(self, enable_logging: bool = False):
        self.api_url = "https://api.cesium.com"
        self.api_asset_url = f"{self.api_url}/v1/assets"
        self.api_archive_url = f"{self.api_url}/v1/archives"
        self.token = os.getenv('CESIUM_ION_TOKEN')
        self.enable_logging = enable_logging
        
        # Set up logging
        self.logger = setup_logging(enable_logging)
        
        if not self.token:
            if enable_logging:
                self._log("error", "CESIUM_ION_TOKEN environment variable not found")
            raise ValueError("CESIUM_ION_TOKEN environment variable is required")
        
        if enable_logging:
            self._log("info", "Cesium ION API Helper initialized")
            self._log("info", f"API URL: {self.api_url}")
        
        self.headers = {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json'
        }
        
        self.results = {
            'success': [],
            'failed': [],
            'archived': []
        }
    
    def _log(self, level: str, message: str) -> None:
        """Helper method for conditional logging."""
        if self.enable_logging:
            getattr(self.logger, level)(message)
    
    def get_gml_files(self, data_folder: str = 'data') -> List[str]:
        """Get all GML files from the data folder."""
        pattern = os.path.join(data_folder, '*.gml')
        gml_files = glob.glob(pattern)
        self._log("info", f"Scanning for GML files in '{data_folder}' folder")
        self._log("info", f"Found {len(gml_files)} GML files: {[Path(f).name for f in gml_files]}")
        return gml_files

    def get_cesium_ion_assets_list(self) -> List[Dict]:
        """
        Fetch the list of assets from Cesium ION.
        
        Returns:
            List of asset dictionaries
        """
        try:
            self._log("info", "Fetching asset list from Cesium ION")
            response = requests.get(
                self.api_asset_url,
                headers=self.headers,
                timeout=30
            )
            response.raise_for_status()
            assets = response.json().get('assets', [])
            self._log("info", f"Successfully fetched {len(assets)} assets from Cesium ION")
            return assets
        except requests.exceptions.RequestException as e:
            self._log("error", f"Error fetching assets: {str(e)}")
            print(f"Error fetching assets: {str(e)}")
            return []

    def get_asset_status(self, asset_id: str) -> Optional[Dict]:
        """
        Get the status of a specific asset by ID.
        
        Args:
            asset_id: The ID of the asset to check
            
        Returns:
            Asset information dictionary or None if error
        """
        try:
            url = f"{self.api_asset_url}/{asset_id}"
            self._log("debug", f"Checking status for asset {asset_id}")
            response = requests.get(
                url,
                headers=self.headers,
                timeout=30
            )
            response.raise_for_status()
            asset_data = response.json()
            status = asset_data.get('status', 'UNKNOWN')
            self._log("debug", f"Asset {asset_id} status: {status}")
            return asset_data
        except requests.exceptions.RequestException as e:
            self._log("error", f"Error fetching asset {asset_id}: {str(e)}")
            print(f"Error fetching asset {asset_id}: {str(e)}")
            return None
    
    def create_asset_metadata(self, file_path: str) -> Optional[Dict]:
        """
        Step 1: Create asset metadata and get upload credentials.
        
        Args:
            file_path: Path to the GML file
            
        Returns:
            Response containing upload location and asset metadata
        """
        filename = Path(file_path).name
        name_without_ext = Path(file_path).stem
        
        self._log("info", f"Step 1: Creating asset metadata for {filename}")
        
        payload = {
            "name": name_without_ext,
            "type": "3DTILES",
            "description": f"Uploaded GML file: {filename} from Cesium Helper script",
            "options": {
                "sourceType": "CITYGML",
                "textureFormat": "KTX2",
                "geometryCompression": "DRACO",
                "clampToTerrain": True,
                "baseTerrainId": 1  # Cesium World Terrain
            }
        }
        
        self._log("debug", f"Asset metadata payload: {json.dumps(payload, indent=2)}")
        
        try:
            response = requests.post(
                self.api_asset_url,
                headers=self.headers,
                json=payload,
                timeout=30
            )
            response.raise_for_status()
            result = response.json()
            asset_id = result['assetMetadata']['id']
            self._log("info", f"✅ Asset metadata created successfully for {filename} (Asset ID: {asset_id})")
            return result
            
        except requests.exceptions.RequestException as e:
            self._log("error", f"❌ Error creating asset metadata for {filename}: {str(e)}")
            print(f"Error creating asset metadata: {str(e)}")
            return None

    def upload_file_to_s3(self, file_path: str, upload_location: Dict) -> bool:
        """
        Step 2: Upload file to Amazon S3 using temporary credentials.
        
        Args:
            file_path: Path to the file to upload
            upload_location: Upload location info from step 1
            
        Returns:
            True if upload successful, False otherwise
        """
        filename = Path(file_path).name
        file_size = Path(file_path).stat().st_size
        file_size_mb = file_size / (1024 * 1024)
        
        self._log("info", f"Step 2: Uploading {filename} to S3 (Size: {file_size_mb:.2f} MB)")
        
        try:
            # Initialize S3 client with temporary credentials
            s3_client = boto3.client(
                's3',
                region_name='us-east-1',
                aws_access_key_id=upload_location['accessKey'],
                aws_secret_access_key=upload_location['secretAccessKey'],
                aws_session_token=upload_location['sessionToken']
            )
            
            s3_key = f"{upload_location['prefix']}{filename}"
            bucket = upload_location['bucket']
            
            self._log("debug", f"S3 upload details - Bucket: {bucket}, Key: {s3_key}")
            
            # Track upload progress
            uploaded_bytes = [0]
            
            def upload_callback(bytes_transferred):
                uploaded_bytes[0] += bytes_transferred
                progress = (uploaded_bytes[0] / file_size) * 100
                if uploaded_bytes[0] % (1024 * 1024) == 0:  # Log every MB
                    self._log("debug", f"Upload progress for {filename}: {progress:.1f}%")
            
            start_time = time.time()
            s3_client.upload_file(
                file_path,
                bucket,
                s3_key,
                Callback=upload_callback
            )
            
            upload_time = time.time() - start_time
            upload_speed = file_size_mb / upload_time if upload_time > 0 else 0
            
            self._log("info", f"✅ Successfully uploaded {filename} to S3 in {upload_time:.2f}s (Speed: {upload_speed:.2f} MB/s)")
            return True
            
        except Exception as e:
            self._log("error", f"❌ Error uploading {filename} to S3: {str(e)}")
            print(f"Error uploading to S3: {str(e)}")
            return False

    def notify_upload_complete(self, on_complete: Dict) -> bool:
        """
        Step 3: Notify Cesium ION that upload is complete.
        
        Args:
            on_complete: Completion info from step 1
            
        Returns:
            True if notification successful, False otherwise
        """
        self._log("info", "Step 3: Notifying Cesium ION that upload is complete")
        
        try:
            response = requests.request(
                method=on_complete['method'],
                url=on_complete['url'],
                headers=self.headers,
                json=on_complete['fields'],
                timeout=30
            )
            response.raise_for_status()
            self._log("info", "✅ Upload completion notification sent successfully")
            return True
            
        except requests.exceptions.RequestException as e:
            self._log("error", f"❌ Error notifying upload complete: {str(e)}")
            print(f"Error notifying upload complete: {str(e)}")
            return False

    def wait_for_processing(self, asset_id: str, timeout: int = 900) -> Tuple[bool, str]:
        """
        Step 4: Monitor asset processing status.
        
        Args:
            asset_id: ID of the asset to monitor
            timeout: Maximum time to wait in seconds (default: 15 minutes)

        Returns:
            Tuple of (success, final_status)
        """
        self._log("info", f"Step 4: Monitoring processing status for asset {asset_id} (timeout: {timeout}s)")
        start_time = time.time()
        last_status = None
        
        while time.time() - start_time < timeout:
            try:
                asset = self.get_asset_status(asset_id)
                if not asset:
                    self._log("error", f"Failed to fetch status for asset {asset_id}")
                    return False, "ERROR_FETCHING_STATUS"
                
                status = asset.get('status', 'UNKNOWN')
                
                # Log status changes
                if status != last_status:
                    elapsed = time.time() - start_time
                    self._log("info", f"Asset {asset_id} status changed to: {status} (after {elapsed:.1f}s)")
                    last_status = status
                
                if status == 'COMPLETE':
                    elapsed = time.time() - start_time
                    self._log("info", f"✅ Asset {asset_id} processing completed successfully in {elapsed:.1f}s")
                    return True, status
                elif status in ['ERROR', 'DATA_ERROR']:
                    elapsed = time.time() - start_time
                    self._log("error", f"❌ Asset {asset_id} processing failed with status: {status} (after {elapsed:.1f}s)")
                    return False, status
                elif status in ['AWAITING_FILES', 'NOT_STARTED', 'IN_PROGRESS']:
                    # Still processing, wait and check again
                    time.sleep(10)
                else:
                    # Unknown status, continue waiting
                    self._log("warning", f"Unknown status '{status}' for asset {asset_id}, continuing to wait...")
                    time.sleep(10)
                    
            except Exception as e:
                self._log("error", f"Error checking status for asset {asset_id}: {str(e)}")
                print(f"Error checking status: {str(e)}")
                time.sleep(10)
        
        elapsed = time.time() - start_time
        self._log("error", f"❌ Timeout waiting for asset {asset_id} processing (waited {elapsed:.1f}s)")
        return False, "TIMEOUT"

    def upload_gml_file(self, file_path: str, wait_for_completion: bool = False, create_archive: bool = False, download_archive: bool = False) -> Tuple[str, bool, str, Optional[str]]:
        """
        Complete upload workflow for a GML file with optional archive creation and download.
        
        Args:
            file_path: Path to the GML file to upload
            wait_for_completion: Whether to wait for processing to complete
            create_archive: Whether to create archive after successful processing
            download_archive: Whether to download archive after successful creation
            
        Returns:
            Tuple of (filename, success_status, message, asset_id)
        """
        filename = Path(file_path).name
        self._log("info", f"Starting upload workflow for {filename}")
        
        try:
            # Step 1: Create asset metadata
            response = self.create_asset_metadata(file_path)
            if not response:
                self._log("error", f"Upload workflow failed for {filename}: Failed to create asset metadata")
                return filename, False, "Failed to create asset metadata", None
            
            asset_id = response['assetMetadata']['id']
            upload_location = response['uploadLocation']
            on_complete = response['onComplete']
            
            # Step 2: Upload file to S3
            if not self.upload_file_to_s3(file_path, upload_location):
                self._log("error", f"Upload workflow failed for {filename}: Failed to upload file to S3")
                return filename, False, "Failed to upload file to S3", str(asset_id)
            
            # Step 3: Notify upload complete
            if not self.notify_upload_complete(on_complete):
                self._log("error", f"Upload workflow failed for {filename}: Failed to notify upload completion")
                return filename, False, "Failed to notify upload completion", str(asset_id)
            
            # Step 4: Optionally wait for processing
            if wait_for_completion:
                self._log("info", f"Waiting for processing completion for {filename} (Asset ID: {asset_id})")
                success, final_status = self.wait_for_processing(str(asset_id))
                if success:
                    # Step 5: Optionally create archive after successful processing
                    if create_archive:
                        self._log("info", f"Creating archive for {filename} (Asset ID: {asset_id})")
                        archive_success, archive_id, archive_message = self.create_archive(str(asset_id))
                        
                        if archive_success and archive_id:
                            # Wait for archive completion
                            archive_completed, archive_status = self.wait_for_archive_completion(archive_id)
                            
                            if archive_completed:
                                # Step 6: Optionally download the archive
                                if download_archive:
                                    self._log("info", f"Downloading archive for {filename} (Archive ID: {archive_id})")
                                    download_success, download_path = self.download_archive(archive_id)
                                    
                                    if download_success:
                                        archive_info = {
                                            'file': filename,
                                            'asset_id': str(asset_id),
                                            'archive_id': archive_id,
                                            'download_path': download_path
                                        }
                                        self.results['archived'].append(archive_info)
                                        self._log("info", f"✅ Complete workflow with archive download success for {filename} (Asset ID: {asset_id}, Archive ID: {archive_id}, Downloaded to: {download_path})")
                                        return filename, True, f"Upload, processing, archive creation and download completed successfully (Asset ID: {asset_id}, Archive ID: {archive_id}, Downloaded to: {download_path})", str(asset_id)
                                    else:
                                        archive_info = {
                                            'file': filename,
                                            'asset_id': str(asset_id),
                                            'archive_id': archive_id,
                                        }
                                        self.results['archived'].append(archive_info)
                                        self._log("warning", f"⚠️ Archive created but download failed for {filename} (Asset ID: {asset_id}, Archive ID: {archive_id})")
                                        return filename, True, f"Upload, processing, and archive completed successfully, but download failed (Asset ID: {asset_id}, Archive ID: {archive_id})", str(asset_id)
                                else:
                                    archive_info = {
                                        'file': filename,
                                        'asset_id': str(asset_id),
                                        'archive_id': archive_id,
                                    }
                                    self.results['archived'].append(archive_info)
                                    self._log("info", f"✅ Complete workflow with archive success for {filename} (Asset ID: {asset_id}, Archive ID: {archive_id})")
                                    return filename, True, f"Upload, processing, and archive completed successfully (Asset ID: {asset_id}, Archive ID: {archive_id})", str(asset_id)
                            else:
                                self._log("warning", f"⚠️ Processing succeeded but archive creation failed for {filename} (Asset ID: {asset_id})")
                                return filename, True, f"Upload and processing completed, but archive failed with status: {archive_status} (Asset ID: {asset_id})", str(asset_id)
                        else:
                            self._log("warning", f"⚠️ Processing succeeded but archive creation failed for {filename} (Asset ID: {asset_id})")
                            return filename, True, f"Upload and processing completed, but archive creation failed: {archive_message} (Asset ID: {asset_id})", str(asset_id)
                    else:
                        self._log("info", f"✅ Complete workflow success for {filename} (Asset ID: {asset_id})")
                        return filename, True, f"Upload and processing completed successfully (Asset ID: {asset_id})", str(asset_id)
                else:
                    self._log("warning", f"⚠️ Upload succeeded but processing failed for {filename} (Asset ID: {asset_id}, Status: {final_status})")
                    return filename, False, f"Upload succeeded but processing failed with status: {final_status} (Asset ID: {asset_id})", str(asset_id)
            else:
                self._log("info", f"✅ Upload workflow completed for {filename} (Asset ID: {asset_id}) - processing will continue in background")
                return filename, True, f"Upload initiated successfully (Asset ID: {asset_id})", str(asset_id)
                
        except Exception as e:
            self._log("error", f"❌ Unexpected error in upload workflow for {filename}: {str(e)}")
            return filename, False, f"Unexpected error: {str(e)}", None
    
    def upload_files_parallel(self, file_paths: List[str], max_workers: int = 10, wait_for_completion: bool = False, create_archive: bool = False, download_archive: bool = False) -> None:
        """
        Upload files in parallel with progress bar and optional archive creation and download.
        
        Args:
            file_paths: List of file paths to upload
            max_workers: Maximum number of concurrent uploads
            wait_for_completion: Whether to wait for processing to complete
            create_archive: Whether to create archives after successful processing
            download_archive: Whether to download archives after successful creation
        """
        self._log("info", f"=== Starting parallel upload session ===")
        self._log("info", f"Files to upload: {len(file_paths)}")
        self._log("info", f"Max workers: {max_workers}")
        self._log("info", f"Wait for completion: {wait_for_completion}")
        self._log("info", f"Create archives: {create_archive}")
        self._log("info", f"Download archives: {download_archive}")
        
        print(f"\n🚀 Starting upload of {len(file_paths)} GML files to Cesium ION...")
        if wait_for_completion:
            print("⏳ Will wait for processing completion...")
        if create_archive:
            print("📦 Will create archives after processing...")
        if download_archive:
            print("📥 Will download archives after creation...")
        
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_file = {
                executor.submit(self.upload_gml_file, file_path, wait_for_completion, create_archive, download_archive): file_path 
                for file_path in file_paths
            }
            
            self._log("info", f"Submitted {len(future_to_file)} upload tasks to thread pool")
            
            # Process completed tasks with progress bar
            with tqdm(total=len(file_paths), desc="Uploading files", unit="file") as pbar:
                for future in as_completed(future_to_file):
                    filename, success, message, asset_id = future.result()
                    
                    if success:
                        result_data = {
                            'file': filename, 
                            'message': message, 
                            'asset_id': asset_id
                        }
                        self.results['success'].append(result_data)
                        self._log("info", f"✅ Upload completed successfully: {filename} -> Asset ID: {asset_id}")
                        pbar.set_postfix_str(f"✅ {filename}")
                    else:
                        result_data = {
                            'file': filename, 
                            'error': message, 
                            'asset_id': asset_id
                        }
                        self.results['failed'].append(result_data)
                        self._log("error", f"❌ Upload failed: {filename} - {message}")
                        pbar.set_postfix_str(f"❌ {filename}")
                    
                    pbar.update(1)
        
        total_time = time.time() - start_time
        success_count = len(self.results['success'])
        failed_count = len(self.results['failed'])
        archived_count = len(self.results['archived'])
        
        self._log("info", f"=== Upload session completed ===")
        self._log("info", f"Total time: {total_time:.2f} seconds")
        self._log("info", f"Successful uploads: {success_count}")
        self._log("info", f"Failed uploads: {failed_count}")
        self._log("info", f"Archived assets: {archived_count}")
        self._log("info", f"Success rate: {(success_count / len(file_paths)) * 100:.1f}%")
    
    def print_summary(self) -> None:
        """Print a summary of upload results including archive and download information."""
        total_files = len(self.results['success']) + len(self.results['failed'])
        success_count = len(self.results['success'])
        failed_count = len(self.results['failed'])
        archived_count = len(self.results['archived'])
        downloaded_count = len([item for item in self.results['archived'] if item.get('download_path')])
        success_rate = (success_count / total_files * 100) if total_files > 0 else 0
        
        # Log summary to file
        self._log("info", f"=== FINAL SUMMARY ===")
        self._log("info", f"Total files processed: {total_files}")
        self._log("info", f"Successful uploads: {success_count}")
        self._log("info", f"Failed uploads: {failed_count}")
        self._log("info", f"Archived assets: {archived_count}")
        self._log("info", f"Downloaded archives: {downloaded_count}")
        self._log("info", f"Success rate: {success_rate:.1f}%")
        
        print("\n" + "="*60)
        print("UPLOAD SUMMARY")
        print("="*60)
        print(f"Total files processed: {total_files}")
        print(f"✅ Successful uploads: {success_count}")
        print(f"❌ Failed uploads: {failed_count}")
        print(f"📦 Archived assets: {archived_count}")
        if downloaded_count > 0:
            print(f"📥 Downloaded archives: {downloaded_count}")
        print(f"📊 Success rate: {success_rate:.1f}%")
        
        if self.results['success']:
            print("\n✅ SUCCESSFUL UPLOADS:")
            print("-" * 40)
            self._log("info", "Successful uploads details:")
            for item in self.results['success']:
                asset_id = item.get('asset_id', 'Unknown')
                print(f"  • {item['file']} (Asset ID: {asset_id})")
                print(f"    View: https://ion.cesium.com/assets/{asset_id}")
                self._log("info", f"  ✅ {item['file']} -> Asset ID: {asset_id}")
        
        if self.results['archived']:
            print("\n📦 ARCHIVED ASSETS:")
            print("-" * 40)
            self._log("info", "Archived assets details:")
            downloaded_archives = []
            created_only_archives = []
            
            for item in self.results['archived']:
                if item.get('download_path'):
                    downloaded_archives.append(item)
                else:
                    created_only_archives.append(item)
            
            if downloaded_archives:
                print("\n📥 DOWNLOADED ARCHIVES:")
                print("-" * 30)
                for item in downloaded_archives:
                    asset_id = item.get('asset_id', 'Unknown')
                    archive_id = item.get('archive_id', 'Unknown')
                    download_path = item.get('download_path')
                    print(f"  • {item['file']} (Asset ID: {asset_id})")
                    print(f"    Archive ID: {archive_id}")
                    print(f"    Downloaded to: {download_path}")
                    print(f"    View Asset: https://ion.cesium.com/assets/{asset_id}")
                    self._log("info", f"  📥 {item['file']} -> Asset ID: {asset_id}, Archive ID: {archive_id}, Downloaded: {download_path}")
            
            if created_only_archives:
                print("\n📦 CREATED (NOT DOWNLOADED) ARCHIVES:")
                print("-" * 40)
                for item in created_only_archives:
                    asset_id = item.get('asset_id', 'Unknown')
                    archive_id = item.get('archive_id', 'Unknown')
                    print(f"  • {item['file']} (Asset ID: {asset_id})")
                    print(f"    Archive ID: {archive_id}")
                    print(f"    View Asset: https://ion.cesium.com/assets/{asset_id}")
                    self._log("info", f"  📦 {item['file']} -> Asset ID: {asset_id}, Archive ID: {archive_id}")
        
        if self.results['failed']:
            print("\n❌ FAILED UPLOADS:")
            print("-" * 40)
            self._log("info", "Failed uploads details:")
            for item in self.results['failed']:
                asset_id = item.get('asset_id', 'N/A')
                print(f"  • {item['file']}: {item['error']}")
                if asset_id and asset_id != 'N/A':
                    print(f"    Asset ID: {asset_id}")
                self._log("error", f"  ❌ {item['file']}: {item['error']} (Asset ID: {asset_id})")
        
        print("\n" + "="*60)
        
        # Log completion
        self._log("info", "=== Upload session completed ===")
        
        # Log path information for user reference
        logs_dir = Path("logs")
        if logs_dir.exists():
            log_files = list(logs_dir.glob("cesium_upload_*.log"))
            if log_files:
                latest_log = max(log_files, key=lambda f: f.stat().st_mtime)
                print(f"📝 Detailed logs saved to: {latest_log}")
                self._log("info", f"Log file location: {latest_log.absolute()}")

    def get_asset_ids_from_results(self) -> List[str]:
        """Get list of asset IDs from successful uploads."""
        asset_ids = []
        for item in self.results['success']:
            asset_id = item.get('asset_id')
            if asset_id:
                asset_ids.append(asset_id)
        return asset_ids

    def create_archive(self, asset_id: str) -> Tuple[bool, Optional[str], str]:
        """
        Create an archive for a processed asset.
        
        Args:
            asset_id: ID of the asset to archive
            archive_name: Optional custom name for the archive
            
        Returns:
            Tuple of (success, archive_id, message)
        """
        self._log("info", f"Step 5: Creating archive for asset {asset_id}")
        

        
        payload = {
            "type": "FULL",
            "format": "ZIP",
            "assetIds": [int(asset_id)]
        }
        
        self._log("debug", f"Archive payload: {json.dumps(payload, indent=2)}")
        
        try:
            response = requests.post(
                self.api_archive_url,
                headers=self.headers,
                json=payload,
                timeout=30
            )
            response.raise_for_status()
            result = response.json()
            archive_id = result.get('id')
            
            if archive_id:
                self._log("info", f"✅ Archive created successfully for asset {asset_id} (Archive ID: {archive_id})")
                return True, str(archive_id), f"Archive created successfully (Archive ID: {archive_id})"
            else:
                self._log("error", f"❌ Archive creation returned no ID for asset {asset_id}")
                return False, None, "Archive creation returned no ID"
                
        except requests.exceptions.RequestException as e:
            self._log("error", f"❌ Error creating archive for asset {asset_id}: {str(e)}")
            return False, None, f"Error creating archive: {str(e)}"

    def wait_for_archive_completion(self, archive_id: str, timeout: int = 300) -> Tuple[bool, str]:
        """
        Monitor archive creation status.
        
        Args:
            archive_id: ID of the archive to monitor
            timeout: Maximum time to wait in seconds (default: 5 minutes)
            
        Returns:
            Tuple of (success, final_status)
        """
        self._log("info", f"Monitoring archive status for archive {archive_id} (timeout: {timeout}s)")
        start_time = time.time()
        last_status = None
        
        while time.time() - start_time < timeout:
            try:
                url = f"{self.api_archive_url}/{archive_id}"
                response = requests.get(
                    url,
                    headers=self.headers,
                    timeout=30
                )
                response.raise_for_status()
                archive_data = response.json()
                status = archive_data.get('status', 'UNKNOWN')
                
                # Log status changes
                if status != last_status:
                    elapsed = time.time() - start_time
                    self._log("info", f"Archive {archive_id} status changed to: {status} (after {elapsed:.1f}s)")
                    last_status = status
                
                if status == 'COMPLETE':
                    elapsed = time.time() - start_time
                    self._log("info", f"✅ Archive {archive_id} creation completed successfully in {elapsed:.1f}s")
                    return True, status
                elif status in ['ERROR', 'FAILED']:
                    elapsed = time.time() - start_time
                    self._log("error", f"❌ Archive {archive_id} creation failed with status: {status} (after {elapsed:.1f}s)")
                    return False, status
                elif status in ['PENDING', 'IN_PROGRESS', 'PROCESSING']:
                    # Still processing, wait and check again
                    time.sleep(5)
                else:
                    # Unknown status, continue waiting
                    self._log("warning", f"Unknown archive status '{status}' for archive {archive_id}, continuing to wait...")
                    time.sleep(5)
                    
            except Exception as e:
                self._log("error", f"Error checking archive status for {archive_id}: {str(e)}")
                time.sleep(5)
        
        elapsed = time.time() - start_time
        self._log("error", f"❌ Timeout waiting for archive {archive_id} creation (waited {elapsed:.1f}s)")
        return False, "TIMEOUT"

    def create_archives_for_completed_assets(self, asset_ids: List[str]) -> None:
        """
        Create archives for a list of already completed assets.
        
        Args:
            asset_ids: List of asset IDs to create archives for
        """
        self._log("info", f"=== Creating archives for {len(asset_ids)} completed assets ===")
        print(f"\n📦 Creating archives for {len(asset_ids)} completed assets...")
        
        completed_assets = []
        failed_assets = []
        
        # First, check which assets are actually completed
        print("🔍 Checking asset statuses...")
        for asset_id in asset_ids:
            asset_data = self.get_asset_status(asset_id)
            if asset_data and asset_data.get('status') == 'COMPLETE':
                completed_assets.append(asset_id)
                self._log("info", f"Asset {asset_id} is ready for archiving")
            else:
                status = asset_data.get('status', 'UNKNOWN') if asset_data else 'ERROR_FETCHING'
                failed_assets.append((asset_id, status))
                self._log("warning", f"Asset {asset_id} not ready for archiving (status: {status})")
        
        if not completed_assets:
            print("❌ No completed assets found for archiving")
            return
        
        print(f"✅ Found {len(completed_assets)} completed assets ready for archiving")
        if failed_assets:
            print(f"⚠️ {len(failed_assets)} assets not ready for archiving")
        
        # Create archives for completed assets
        with tqdm(total=len(completed_assets), desc="Creating archives", unit="archive") as pbar:
            for asset_id in completed_assets:
                try:
                    # Create archive
                    success, archive_id, message = self.create_archive(asset_id)
                    
                    if success and archive_id:
                        # Wait for archive completion
                        archive_completed, archive_status = self.wait_for_archive_completion(archive_id)
                        
                        if archive_completed:
                            archive_info = {
                                'asset_id': asset_id,
                                'archive_id': archive_id,
                            }
                            self.results['archived'].append(archive_info)
                            self._log("info", f"✅ Archive created successfully for asset {asset_id} (Archive ID: {archive_id})")
                            pbar.set_postfix_str(f"✅ Asset {asset_id}")
                        else:
                            self._log("error", f"❌ Archive creation failed for asset {asset_id} (status: {archive_status})")
                            pbar.set_postfix_str(f"❌ Asset {asset_id}")
                    else:
                        self._log("error", f"❌ Failed to create archive for asset {asset_id}: {message}")
                        pbar.set_postfix_str(f"❌ Asset {asset_id}")
                        
                except Exception as e:
                    self._log("error", f"❌ Unexpected error creating archive for asset {asset_id}: {str(e)}")
                    pbar.set_postfix_str(f"❌ Asset {asset_id}")
                
                pbar.update(1)
        
        # Print results
        successful_archives = len(self.results['archived'])
        print(f"\n📦 Archive creation completed: {successful_archives}/{len(completed_assets)} archives created successfully")
        
        if self.results['archived']:
            print("\n📦 CREATED ARCHIVES:")
            print("-" * 40)
            for item in self.results['archived']:
                asset_id = item.get('asset_id', 'Unknown')
                archive_id = item.get('archive_id', 'Unknown')
                download_url = item.get('download_url')
                print(f"  • Asset {asset_id} -> Archive {archive_id}")
                if download_url:
                    print(f"    Download: {download_url}")
                print(f"    View Asset: https://ion.cesium.com/assets/{asset_id}")

    def download_archive(self, archive_id: str, output_dir: str = "converted") -> Tuple[bool, Optional[str]]:
        """
        Download an archive from Cesium ION and save it to the specified directory.
        
        Args:
            archive_id: ID of the archive to download
            output_dir: Directory to save the downloaded archive (default: "converted")
            
        Returns:
            Tuple of (success, downloaded_file_path)
        """
        self._log("info", f"Downloading archive {archive_id}...")
        
        try:
            # Ensure output directory exists
            output_path = Path(output_dir)
            output_path.mkdir(exist_ok=True)
            
            # Get the archive details first to check status and get metadata
            archive_info = self.get_archive_info(archive_id)
            if not archive_info:
                self._log("error", f"Could not retrieve archive info for archive {archive_id}")
                return False, None
                
            if archive_info.get('status') != 'COMPLETE':
                self._log("error", f"Archive {archive_id} is not ready for download (status: {archive_info.get('status')})")
                return False, None
            
            # Request download URL from Cesium ION API
            download_url_endpoint = f"{self.api_archive_url}/{archive_id}/download"
            self._log("debug", f"Requesting download URL from: {download_url_endpoint}")
            
            response = requests.get(
                download_url_endpoint,
                headers=self.headers,
                timeout=30
            )
            response.raise_for_status()
            
            # Check if response contains a redirect URL or direct download
            if response.headers.get('content-type', '').startswith('application/json'):
                # Response contains JSON with download URL
                download_data = response.json()
                download_url = download_data.get('url') or download_data.get('downloadUrl')
                if not download_url:
                    self._log("error", f"No download URL found in response for archive {archive_id}")
                    return False, None
            else:
                # Direct download response
                download_url = response.url
            
            self._log("debug", f"Download URL obtained for archive {archive_id}")
            
            # Download the archive file with progress tracking
            archive_name = archive_info.get('name', f'archive_{archive_id}')
            # Clean filename for filesystem
            safe_name = "".join(c for c in archive_name if c.isalnum() or c in (' ', '-', '_', '.')).rstrip()
            if not safe_name:
                safe_name = f'archive_{archive_id}'
            
            archive_file_path = output_path / f"{safe_name}.zip"
            
            # Handle duplicate filenames
            counter = 1
            while archive_file_path.exists():
                name_part = archive_file_path.stem
                archive_file_path = output_path / f"{name_part}_{counter}.zip"
                counter += 1
            
            self._log("info", f"Downloading archive to: {archive_file_path}")
            
            # Download with progress tracking
            download_response = requests.get(download_url, stream=True)
            download_response.raise_for_status()
            
            total_size = int(download_response.headers.get('content-length', 0))
            
            with open(archive_file_path, 'wb') as f:
                if total_size > 0:
                    with tqdm(total=total_size, unit='B', unit_scale=True, desc=f"Downloading {archive_file_path.name}") as pbar:
                        for chunk in download_response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                                pbar.update(len(chunk))
                else:
                    for chunk in download_response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
            
            # Verify file was downloaded
            if archive_file_path.exists() and archive_file_path.stat().st_size > 0:
                file_size_mb = archive_file_path.stat().st_size / (1024 * 1024)
                self._log("info", f"✅ Archive {archive_id} downloaded successfully: {archive_file_path} ({file_size_mb:.2f} MB)")
                return True, str(archive_file_path)
            else:
                self._log("error", f"❌ Downloaded file is empty or doesn't exist: {archive_file_path}")
                return False, None
                
        except requests.exceptions.RequestException as e:
            self._log("error", f"❌ Error downloading archive {archive_id}: {str(e)}")
            return False, None
        except Exception as e:
            self._log("error", f"❌ Unexpected error downloading archive {archive_id}: {str(e)}")
            return False, None

    def get_archive_info(self, archive_id: str) -> Optional[Dict]:
        """
        Get detailed information about a specific archive.
        
        Args:
            archive_id: ID of the archive to get info for
            
        Returns:
            Archive information dictionary or None if error
        """
        try:
            url = f"{self.api_archive_url}/{archive_id}"
            self._log("debug", f"Fetching archive info from: {url}")
            
            response = requests.get(
                url,
                headers=self.headers,
                timeout=30
            )
            response.raise_for_status()
            archive_data = response.json()
            
            self._log("debug", f"Archive {archive_id} info retrieved successfully")
            return archive_data
            
        except requests.exceptions.RequestException as e:
            self._log("error", f"Error fetching archive info for {archive_id}: {str(e)}")
            return None

    def download_all_completed_archives(self, output_dir: str = "converted") -> List[Dict]:
        """
        Download all completed archives from Cesium ION.
        
        Args:
            output_dir: Directory to save downloaded archives (default: "converted")
            
        Returns:
            List of download results with details
        """
        self._log("info", "Downloading all completed archives...")
        
        # Get list of all archives
        archived_assets = self.list_archived_assets()
        if not archived_assets:
            self._log("warning", "No archives found")
            print("❌ No archives found")
            return []
        
        # Filter for completed archives
        completed_archives = [
            archive for archive in archived_assets 
            if archive.get('status') == 'COMPLETE'
        ]
        
        if not completed_archives:
            self._log("warning", "No completed archives found")
            print("❌ No completed archives found for download")
            return []
        
        print(f"📦 Found {len(completed_archives)} completed archives for download")
        self._log("info", f"Found {len(completed_archives)} completed archives for download")
        
        download_results = []
        
        # Download each archive
        with tqdm(total=len(completed_archives), desc="Downloading archives", unit="archive") as pbar:
            for archive in completed_archives:
                archive_id = archive.get('id')
                archive_name = archive.get('name', f'Archive {archive_id}')
                
                try:
                    success, file_path = self.download_archive(archive_id, output_dir)
                    
                    result = {
                        'archive_id': archive_id,
                        'name': archive_name,
                        'success': success,
                        'file_path': file_path,
                        'size_mb': archive.get('size', 0)
                    }
                    
                    if success:
                        result['message'] = f"Downloaded successfully to {file_path}"
                        pbar.set_postfix_str(f"✅ {archive_name}")
                    else:
                        result['message'] = "Download failed"
                        pbar.set_postfix_str(f"❌ {archive_name}")
                    
                    download_results.append(result)
                    
                except Exception as e:
                    result = {
                        'archive_id': archive_id,
                        'name': archive_name,
                        'success': False,
                        'file_path': None,
                        'message': f"Unexpected error: {str(e)}"
                    }
                    download_results.append(result)
                    self._log("error", f"Unexpected error downloading archive {archive_id}: {str(e)}")
                    pbar.set_postfix_str(f"❌ {archive_name}")
                
                pbar.update(1)
        
        # Print summary
        successful_downloads = len([r for r in download_results if r['success']])
        failed_downloads = len([r for r in download_results if not r['success']])
        
        print(f"\n📥 Download completed: {successful_downloads}/{len(completed_archives)} archives downloaded successfully")
        
        if successful_downloads > 0:
            print("\n✅ SUCCESSFULLY DOWNLOADED ARCHIVES:")
            print("-" * 50)
            for result in download_results:
                if result['success']:
                    print(f"  • {result['name']} (ID: {result['archive_id']})")
                    print(f"    File: {result['file_path']}")
                    if result.get('size_mb'):
                        print(f"    Size: {result['size_mb']:.2f} MB")
        
        if failed_downloads > 0:
            print("\n❌ FAILED DOWNLOADS:")
            print("-" * 30)
            for result in download_results:
                if not result['success']:
                    print(f"  • {result['name']} (ID: {result['archive_id']}): {result['message']}")
        
        return download_results
