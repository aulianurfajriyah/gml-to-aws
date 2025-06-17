#!/usr/bin/env python3
"""
Cesium ION API Helper

This module provides helper functions for interacting with Cesium ION API.
Implements the complete 4-step upload workflow:
1. Create asset metadata
2. Upload files to S3
3. Notify completion
4. Monitor status
"""

import os
import glob
import json
import time
import boto3
import requests
import logging
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
            'failed': []
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

    def upload_gml_file(self, file_path: str, wait_for_completion: bool = False) -> Tuple[str, bool, str, Optional[str]]:
        """
        Complete 4-step upload workflow for a GML file.
        
        Args:
            file_path: Path to the GML file to upload
            wait_for_completion: Whether to wait for processing to complete
            
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
    
    def upload_files_parallel(self, file_paths: List[str], max_workers: int = 10, wait_for_completion: bool = False) -> None:
        """
        Upload files in parallel with progress bar.
        
        Args:
            file_paths: List of file paths to upload
            max_workers: Maximum number of concurrent uploads
            wait_for_completion: Whether to wait for processing to complete
        """
        self._log("info", f"=== Starting parallel upload session ===")
        self._log("info", f"Files to upload: {len(file_paths)}")
        self._log("info", f"Max workers: {max_workers}")
        self._log("info", f"Wait for completion: {wait_for_completion}")
        
        print(f"\n🚀 Starting upload of {len(file_paths)} GML files to Cesium ION...")
        if wait_for_completion:
            print("⏳ Will wait for processing completion...")
        
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_file = {
                executor.submit(self.upload_gml_file, file_path, wait_for_completion): file_path 
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
        
        self._log("info", f"=== Upload session completed ===")
        self._log("info", f"Total time: {total_time:.2f} seconds")
        self._log("info", f"Successful uploads: {success_count}")
        self._log("info", f"Failed uploads: {failed_count}")
        self._log("info", f"Success rate: {(success_count / len(file_paths)) * 100:.1f}%")
    
    def print_summary(self) -> None:
        """Print a summary of upload results."""
        total_files = len(self.results['success']) + len(self.results['failed'])
        success_count = len(self.results['success'])
        failed_count = len(self.results['failed'])
        success_rate = (success_count / total_files * 100) if total_files > 0 else 0
        
        # Log summary to file
        self._log("info", f"=== FINAL SUMMARY ===")
        self._log("info", f"Total files processed: {total_files}")
        self._log("info", f"Successful uploads: {success_count}")
        self._log("info", f"Failed uploads: {failed_count}")
        self._log("info", f"Success rate: {success_rate:.1f}%")
        
        print("\n" + "="*60)
        print("UPLOAD SUMMARY")
        print("="*60)
        print(f"Total files processed: {total_files}")
        print(f"✅ Successful uploads: {success_count}")
        print(f"❌ Failed uploads: {failed_count}")
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
