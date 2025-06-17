#!/bin/bash

echo "🔧 Setting up GML Cesium Uploader Environment"
echo "=============================================="

# Create virtual environment
echo "📦 Creating virtual environment..."
python3 -m venv venv

# Activate virtual environment
echo "🔌 Activating virtual environment..."
source venv/bin/activate

# Upgrade pip
echo "⬆️  Upgrading pip..."
pip install --upgrade pip

# Install requirements
echo "📚 Installing required packages..."
pip install -r requirements.txt

echo ""
echo "✅ Setup complete!"
echo ""
echo "🚀 To use the uploader:"
echo "1. Activate the virtual environment: source venv/bin/activate"
echo "2. Set your Cesium ION token in .env file: CESIUM_ION_TOKEN=your_token_here"
echo "3. Run the script: python main.py"
echo ""
echo "💡 Don't forget to deactivate when done: deactivate"
