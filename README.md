# GML to Cesium ION Uploader

This Python script uploads GML files from the `data` folder to Cesium ION API with parallel processing and optional archive creation.

## Quick Start

### 1. Setup Virtual Environment

Run the setup script to create a virtual environment and install dependencies:

**Linux/macOS:**

```bash
chmod +x setup.sh
./setup.sh
```

**Windows:**

```cmd
setup.bat
```

### 2. Configure API Token

Create .env file in the root folder and then copy the example environment file content inside it.
Edit `.env` and replace `your_cesium_ion_token_here` with your actual Cesium ION API token.

You can get your token from: https://cesium.com/ion/tokens

### 3. Add GML Files

Place your GML files in the `data` folder. The script will automatically find all `.gml` files.

### 4. Run the Uploader

```bash
# Activate virtual environment
source venv/bin/activate

# Basic usage - upload files without logging
python main.py

# Upload with detailed logging
python main.py --logging

# Upload and wait for processing completion
python main.py --wait

# Upload, wait for processing, and create archives
python main.py --wait --archive

# Upload, wait for processing, create archives, and download them
python main.py --wait --archive --download

# Upload with custom number of workers
python main.py --workers 3

# Combine options - complete workflow with downloads and logging
python main.py --wait --archive --download --logging --workers 5

# Upload subgrids only
python main.py --upload2S3

# Upload subgrids with logging
python main.py --upload2S3 --logging

# Complete workflow: Cesium ION + 3D tiles upload
python main.py --wait --archive --download --upload2S3 --logging

# Deactivate when done
deactivate
```

## Command Line Options

- `--wait`: Wait for processing completion (can take several minutes)
- `--archive`: Create archives after successful processing so it can be downloaded later (requires --wait)
- `--download`: Download archives to 'converted' folder after creation (requires --archive)
- `--workers N`: Number of concurrent uploads (default: 5)
- `--logging`: Enable detailed logging to file and console (default: disabled)
- `--upload2S3` : Upload converted 3dtiles to AWS S3 bucket

## Logging

By default, logging is **disabled** to keep the output clean. When you use the `--logging` flag:

- Detailed logs are written to `logs/cesium_upload_TIMESTAMP.log`
- Logs are also displayed in the console
- Includes upload progress, timing information, and detailed error messages

Example with logging enabled:

```bash
python main.py --logging --wait
```

## Additional Scripts

### Asset Status Checker

Check the status of uploaded assets:

```bash
# Check specific assets
python check_status.py 12345 67890

# List recent assets
python check_status.py --list --limit 10

# Monitor assets until completion
python check_status.py --monitor 12345 67890
```

### Archive Downloader

Download archives from Cesium ION:

```bash
# Download all completed archives
python download_archives.py

# Download specific archives
python download_archives.py --archive-ids 123 456

# Download to custom directory
python download_archives.py --output-dir downloads

# List available archives without downloading
python download_archives.py --list-only

# Download with logging
python download_archives.py --logging
```

## Workflow Overview

The complete workflow includes these steps:

1. **Upload**: Create asset metadata and upload GML files to S3
2. **Processing**: Monitor 3D tiling process until completion
3. **Archive Creation** (optional): Create downloadable archives
4. **Archive Download** (optional): Download archives to 'converted' folder
5. **Status Tracking**: Monitor all steps with detailed logging

## Output Folders

- `data/`: Place your GML files here for upload
- `converted/`: Downloaded archives are saved here (when using --download)
- `logs/`: Detailed log files (when using --logging)
- `temp/`: Temporary files during processing
