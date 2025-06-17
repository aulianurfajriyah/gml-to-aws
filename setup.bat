@echo off
echo 🔧 Setting up GML Cesium Uploader Environment (Windows)
echo ==================================================

echo 📦 Creating virtual environment...
python -m venv venv

echo 🔌 Activating virtual environment...
call venv\Scripts\activate.bat

echo ⬆️  Upgrading pip...
python -m pip install --upgrade pip

echo 📚 Installing required packages...
pip install -r requirements.txt

echo.
echo ✅ Setup complete!
echo.
echo 🚀 To use the uploader:
echo 1. Activate the virtual environment: venv\Scripts\activate.bat
echo 2. Set your Cesium ION token in .env file: CESIUM_ION_TOKEN=your_token_here
echo 3. Run the script: python main.py
echo.
echo 💡 Don't forget to deactivate when done: deactivate
pause
