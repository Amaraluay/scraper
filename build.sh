#!/usr/bin/env bash

echo "📦 Installing dependencies..."
pip install --upgrade pip
pip install -r requirments.txt

echo "🧩 Installing Playwright browsers..."
python -m playwright install --with-deps chromium
