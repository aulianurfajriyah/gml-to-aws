#!/usr/bin/env python3
"""
Cesium ION API Helper

This module provides helper functions for interacting with Cesium ION API.
"""

import os
import glob
import json
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
    
    def upload_gml_file(self, file_path: str) -> Tuple[str, bool, str]:
        """
        Upload a single GML file to Cesium ION.
        
        Args:
            file_path: Path to the GML file to upload
            
        Returns:
            Tuple of (filename, success_status, message)
        """
        filename = Path(file_path).name
        name_without_ext = Path(file_path).stem
        
        payload = {
            "name": name_without_ext,
            "type": "3DTILES",
            "description": f"Uploaded GML file: {filename} from Cesium Helper script",
            "options": {
                "sourceType": "CITYGML"
            }
        }
        
        try:
            response = requests.post(
                self.api_asset_url,
                headers=self.headers,
                json=payload,
                timeout=30
            )
                        
            if response.status_code == 200 or response.status_code == 201:
                return filename, True, f"Successfully uploaded {filename}"
            else:
                error_msg = f"HTTP {response.status_code}: {response.text}"
                return filename, False, error_msg
                
        except requests.exceptions.RequestException as e:
            return filename, False, f"Request failed: {str(e)}"
        except Exception as e:
            return filename, False, f"Unexpected error: {str(e)}"
    
    def upload_files_parallel(self, file_paths: List[str], max_workers: int = 10) -> None:
        """
        Upload files in parallel with progress bar.
        
        Args:
            file_paths: List of file paths to upload
            max_workers: Maximum number of concurrent uploads
        """
        print(f"\n🚀 Starting upload of {len(file_paths)} GML files to Cesium ION...")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_file = {
                executor.submit(self.upload_gml_file, file_path): file_path 
                for file_path in file_paths
            }
            
            # Process completed tasks with progress bar
            with tqdm(total=len(file_paths), desc="Uploading files", unit="file") as pbar:
                for future in as_completed(future_to_file):
                    filename, success, message = future.result()
                    
                    if success:
                        self.results['success'].append({'file': filename, 'message': message})
                        pbar.set_postfix_str(f"✅ {filename}")
                    else:
                        self.results['failed'].append({'file': filename, 'error': message})
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
                print(f"  • {item['file']}")
        
        if self.results['failed']:
            print("\n❌ FAILED UPLOADS:")
            print("-" * 40)
            for item in self.results['failed']:
                print(f"  • {item['file']}: {item['error']}")
        
        print("\n" + "="*60)
