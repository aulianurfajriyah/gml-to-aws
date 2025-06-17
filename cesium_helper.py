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
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Tuple, Optional
from tqdm import tqdm
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class CesiumAPIHelper:
    def __init__(self):
        self.api_url = "https://api.cesium.com"
        self.api_asset_url = f"{self.api_url}/v1/assets"
        self.token = os.getenv('CESIUM_ION_TOKEN')
        
        if not self.token:
            raise ValueError("CESIUM_ION_TOKEN environment variable is required")
        
        self.headers = {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json'
        }
        
        self.results = {
            'success': [],
            'failed': []
        }
    
    def get_gml_files(self, data_folder: str = 'data') -> List[str]:
        """Get all GML files from the data folder."""
        pattern = os.path.join(data_folder, '*.gml')
        return glob.glob(pattern)

    def get_cesium_ion_assets_list(self) -> List[Dict]:
        """
        Fetch the list of assets from Cesium ION.
        
        Returns:
            List of asset dictionaries
        """
        try:
            response = requests.get(
                self.api_asset_url,
                headers=self.headers,
                timeout=30
            )
            response.raise_for_status()
            return response.json().get('assets', [])
        except requests.exceptions.RequestException as e:
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
            response = requests.get(
                url,
                headers=self.headers,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
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
        
        try:
            response = requests.post(
                self.api_asset_url,
                headers=self.headers,
                json=payload,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
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
        try:
            # Initialize S3 client with temporary credentials
            s3_client = boto3.client(
                's3',
                region_name='us-east-1',
                aws_access_key_id=upload_location['accessKey'],
                aws_secret_access_key=upload_location['secretAccessKey'],
                aws_session_token=upload_location['sessionToken']
            )
            
            filename = Path(file_path).name
            s3_key = f"{upload_location['prefix']}{filename}"
            
            # Upload file with progress callback
            def upload_callback(bytes_transferred):
                pass  # Could add progress tracking here if needed
            
            s3_client.upload_file(
                file_path,
                upload_location['bucket'],
                s3_key,
                Callback=upload_callback
            )
            
            return True
            
        except Exception as e:
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
        try:
            response = requests.request(
                method=on_complete['method'],
                url=on_complete['url'],
                headers=self.headers,
                json=on_complete['fields'],
                timeout=30
            )
            response.raise_for_status()
            return True
            
        except requests.exceptions.RequestException as e:
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
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                asset = self.get_asset_status(asset_id)
                if not asset:
                    return False, "ERROR_FETCHING_STATUS"
                
                status = asset.get('status', 'UNKNOWN')
                
                if status == 'COMPLETE':
                    return True, status
                elif status in ['ERROR', 'DATA_ERROR']:
                    return False, status
                elif status in ['AWAITING_FILES', 'NOT_STARTED', 'IN_PROGRESS']:
                    # Still processing, wait and check again
                    time.sleep(10)
                else:
                    # Unknown status, continue waiting
                    time.sleep(10)
                    
            except Exception as e:
                print(f"Error checking status: {str(e)}")
                time.sleep(10)
                
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
        
        try:
            # Step 1: Create asset metadata
            response = self.create_asset_metadata(file_path)
            if not response:
                return filename, False, "Failed to create asset metadata", None
            
            asset_id = response['assetMetadata']['id']
            upload_location = response['uploadLocation']
            on_complete = response['onComplete']
            
            # Step 2: Upload file to S3
            if not self.upload_file_to_s3(file_path, upload_location):
                return filename, False, "Failed to upload file to S3", str(asset_id)
            
            # Step 3: Notify upload complete
            if not self.notify_upload_complete(on_complete):
                return filename, False, "Failed to notify upload completion", str(asset_id)
            
            # Step 4: Optionally wait for processing
            if wait_for_completion:
                success, final_status = self.wait_for_processing(str(asset_id))
                if success:
                    return filename, True, f"Upload and processing completed successfully (Asset ID: {asset_id})", str(asset_id)
                else:
                    return filename, False, f"Upload succeeded but processing failed with status: {final_status} (Asset ID: {asset_id})", str(asset_id)
            else:
                return filename, True, f"Upload initiated successfully (Asset ID: {asset_id})", str(asset_id)
                
        except Exception as e:
            return filename, False, f"Unexpected error: {str(e)}", None
    
    def upload_files_parallel(self, file_paths: List[str], max_workers: int = 10, wait_for_completion: bool = False) -> None:
        """
        Upload files in parallel with progress bar.
        
        Args:
            file_paths: List of file paths to upload
            max_workers: Maximum number of concurrent uploads
            wait_for_completion: Whether to wait for processing to complete
        """
        print(f"\n🚀 Starting upload of {len(file_paths)} GML files to Cesium ION...")
        if wait_for_completion:
            print("⏳ Will wait for processing completion...")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_file = {
                executor.submit(self.upload_gml_file, file_path, wait_for_completion): file_path 
                for file_path in file_paths
            }
            
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
                        pbar.set_postfix_str(f"✅ {filename}")
                    else:
                        result_data = {
                            'file': filename, 
                            'error': message, 
                            'asset_id': asset_id
                        }
                        self.results['failed'].append(result_data)
                        pbar.set_postfix_str(f"❌ {filename}")
                    
                    pbar.update(1)
    
    def print_summary(self) -> None:
        """Print a summary of upload results."""
        total_files = len(self.results['success']) + len(self.results['failed'])
        success_count = len(self.results['success'])
        failed_count = len(self.results['failed'])
        
        print("\n" + "="*60)
        print("UPLOAD SUMMARY")
        print("="*60)
        print(f"Total files processed: {total_files}")
        print(f"✅ Successful uploads: {success_count}")
        print(f"❌ Failed uploads: {failed_count}")
        
        if self.results['success']:
            print("\n✅ SUCCESSFUL UPLOADS:")
            print("-" * 40)
            for item in self.results['success']:
                asset_id = item.get('asset_id', 'Unknown')
                print(f"  • {item['file']} (Asset ID: {asset_id})")
                print(f"    View: https://ion.cesium.com/assets/{asset_id}")
        
        if self.results['failed']:
            print("\n❌ FAILED UPLOADS:")
            print("-" * 40)
            for item in self.results['failed']:
                asset_id = item.get('asset_id', 'N/A')
                print(f"  • {item['file']}: {item['error']}")
                if asset_id and asset_id != 'N/A':
                    print(f"    Asset ID: {asset_id}")
        
        print("\n" + "="*60)

    def get_asset_ids_from_results(self) -> List[str]:
        """Get list of asset IDs from successful uploads."""
        asset_ids = []
        for item in self.results['success']:
            asset_id = item.get('asset_id')
            if asset_id:
                asset_ids.append(asset_id)
        return asset_ids
