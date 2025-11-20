#!/bin/bash

echo "=========================================="
echo "Reddit Scraper API - Starting Server"
echo "=========================================="

# Check if envActive exists
if [ ! -f "envActive" ]; then
    echo "❌ Error: envActive file not found!"
    echo "Please create envActive file with your Reddit accounts"
    exit 1
fi

# Check if data-universe exists
if [ ! -d "data-universe" ]; then
    echo "⚠️  Warning: data-universe directory not found"
    echo "Make sure it's in the same directory as the API"
fi

# Install dependencies
echo "Installing dependencies..."
pip install -r requirements_api.txt

# Create logs directory
mkdir -p logs

# Start server
echo "Starting API server on http://0.0.0.0:8000"
echo "Press Ctrl+C to stop"
echo ""

python reddit_scraper_api.py