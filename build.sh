#!/bin/bash
echo "📦 Installing dependencies..."
pip install -r requirments.txt

pip install --upgrade pip
pip install playwright

# Installiere Playwright-Browser manuell
echo "🧩 Installiere Chromium..."
python -m playwright install chromium || exit 1
