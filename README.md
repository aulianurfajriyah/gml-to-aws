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

# Upload with custom number of workers
python main.py --workers 3

# Combine options
python main.py --wait --archive --logging --workers 5

# Deactivate when done
deactivate
```

## Command Line Options

- `--wait`: Wait for processing completion (can take several minutes)
- `--archive`: Create archives after successful processing so it can be downloaded later (requires --wait)
- `--workers N`: Number of concurrent uploads (default: 5)
- `--logging`: Enable detailed logging to file and console (default: disabled)

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

## Workflow Overview

The complete workflow includes these steps:

1. **Upload**: Create asset metadata and upload GML files to S3
2. **Processing**: Monitor 3D tiling process until completion
3. **Archive Creation** (optional): Create downloadable archives
4. **Status Tracking**: Monitor all steps with detailed logging
