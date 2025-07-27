#!/usr/bin/env bash

echo "📦 Installiere Python-Pakete..."
pip install --upgrade pip
pip install -r Requirments.txt

echo "🧩 Installiere Playwright-Browser..."
python -m playwright install chromium
