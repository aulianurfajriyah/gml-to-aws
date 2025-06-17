# GML to Cesium ION Uploader

This Python script uploads GML files from the `data` folder to Cesium ION API paralelly

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

# Run the uploader
python main.py

# Deactivate when done
deactivate
```
