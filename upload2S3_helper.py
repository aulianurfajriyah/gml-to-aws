import zipfile
import pandas as pd
import requests
import os
import tempfile
import shutil
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

def upload_subgrids_bulk(enable_logging=False):
    """
    Bulk upload subgrids to the 3D tiles API from converted folder
    """
    
    # Set up logging
    logger = logging.getLogger('upload_helper')
    if enable_logging:
        logger.setLevel(logging.INFO)
    else:
        logger.setLevel(logging.CRITICAL + 1)
    
    def log_if_enabled(level: str, message: str):
        """Helper function for conditional logging."""
        if enable_logging:
            getattr(logger, level)(message)
    
    # Configuration - Load API key from environment
    API_URL = "https://dt-ugm-api-production.up.railway.app/3d-tiles"
    API_KEY = os.getenv("UGM_API_KEY")
    
    if not API_KEY:
        raise ValueError("UGM_API_KEY not found in environment variables")
    
    headers = {
        "Authorization": f"ApiKey {API_KEY}"
    }
    
    print("=== Bulk 3D Tiles Subgrid Uploader ===\n")
    log_if_enabled("info", "Starting bulk upload process")
    
    # Define paths
    converted_folder = Path("/Volumes/DATA_Server11/01 DKI JAKARTA 2025/03 SKETCHUP/06 PERCEPATAN 50%/GML/RESULT_SCRIPT")
    csv_file = Path("centroid.csv")
    
    # Check if converted folder exists
    if not converted_folder.exists():
        print("❌ 'converted' folder not found.")
        log_if_enabled("error", "Converted folder not found")
        return [], []
    
    # Check if CSV file exists
    if not csv_file.exists():
        print("❌ 'centroid.csv' file not found.")
        log_if_enabled("error", "centroid.csv file not found")
        return [], []
    
    # Find zip files in converted folder
    zip_files = list(converted_folder.glob("*.zip"))
    
    if not zip_files:
        print("❌ No ZIP files found in 'converted' folder.")
        log_if_enabled("warning", "No ZIP files found in converted folder")
        return [], []
    
    print(f"📁 Found {len(zip_files)} ZIP files in 'converted' folder")
    log_if_enabled("info", f"Found {len(zip_files)} ZIP files")
    
    # Read CSV file
    try:
        df = pd.read_csv(csv_file)
        print(f"📊 CSV loaded successfully. Found {len(df)} records.")
        print("CSV columns:", df.columns.tolist())
        log_if_enabled("info", f"CSV loaded with {len(df)} records")
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        log_if_enabled("error", f"Error reading CSV: {e}")
        return [], []
    
    # Process each zip file
    successful_uploads = []
    failed_uploads = []
    
    for zip_file in zip_files:
        try:
            # Extract filename without extension for matching with CSV
            base_name = zip_file.stem
            
            # Find matching row in CSV
            matching_rows = df[df.iloc[:, 0].str.contains(base_name, case=False, na=False)]
            
            if matching_rows.empty:
                print(f"⚠️  No matching CSV entry found for {zip_file.name}, skipping...")
                failed_uploads.append({"file": zip_file.name, "error": "No matching CSV entry"})
                log_if_enabled("warning", f"No CSV match for {zip_file.name}")
                continue
            
            # Get coordinates from CSV (assuming columns are: name, center_x, center_y)
            row = matching_rows.iloc[0]
            name = str(row.iloc[0])  # First column as name
            center_x = str(row.iloc[1])  # Second column as center_x
            center_y = str(row.iloc[2])  # Third column as center_y
            
            print(f"\n📤 Uploading {zip_file.name}...")
            print(f"   Name: {name}")
            print(f"   Center X: {center_x}")
            print(f"   Center Y: {center_y}")
            
            log_if_enabled("info", f"Uploading {zip_file.name} - {name}")
            
            # Get current timestamp
            now = datetime.now().isoformat()
            
            # Prepare multipart form data
            with open(zip_file, 'rb') as f:
                files_data = {'file': (zip_file.name, f, 'application/zip')}
                
                form_data = {
                    'name': name,
                    'center_x': center_x,
                    'center_y': center_y,
                    'status': 'true',
                    'url': '',  # Optional field
                    'category': 'building',
                    'createdAt': now,
                    'updatedAt': now
                }
                
                # Make POST request
                response = requests.post(
                    API_URL,
                    headers=headers,
                    files=files_data,
                    data=form_data,
                    timeout=300  # 5 minutes timeout
                )
            
            if response.status_code in [200, 201]:
                print(f"   ✅ Successfully uploaded {zip_file.name}")
                successful_uploads.append({
                    "file": zip_file.name,
                    "name": name,
                    "response": response.json() if response.content else "Success"
                })
                log_if_enabled("info", f"Successfully uploaded {zip_file.name}")
            else:
                error_msg = f"HTTP {response.status_code}: {response.text}"
                print(f"   ❌ Failed to upload {zip_file.name}: {error_msg}")
                failed_uploads.append({
                    "file": zip_file.name,
                    "error": error_msg
                })
                log_if_enabled("error", f"Failed to upload {zip_file.name}: {error_msg}")
        
        except Exception as e:
            error_msg = str(e)
            print(f"   ❌ Error processing {zip_file.name}: {error_msg}")
            failed_uploads.append({
                "file": zip_file.name,
                "error": error_msg
            })
            log_if_enabled("error", f"Error processing {zip_file.name}: {error_msg}")
    
    # Summary report
    print("\n" + "="*50)
    print("📊 UPLOAD SUMMARY")
    print("="*50)
    print(f"✅ Successful uploads: {len(successful_uploads)}")
    print(f"❌ Failed uploads: {len(failed_uploads)}")
    
    if successful_uploads:
        print("\n✅ Successfully uploaded files:")
        for upload in successful_uploads:
            print(f"   - {upload['file']} ({upload['name']})")
    
    if failed_uploads:
        print("\n❌ Failed uploads:")
        for failure in failed_uploads:
            print(f"   - {failure['file']}: {failure['error']}")
    
    log_if_enabled("info", f"Upload process completed: {len(successful_uploads)} successful, {len(failed_uploads)} failed")
    
    print(f"\n🎉 Bulk upload process completed!")
    return successful_uploads, failed_uploads
