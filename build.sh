#!/bin/bash
echo "📦 Installing dependencies..."
pip install -r requirments.txt

echo "🧩 Installing Playwright browsers..."
playwright install chromium
