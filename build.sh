#!/bin/bash
echo "📦 Installing dependencies..."
pip install -r requirements.txt

pip install --upgrade pip
pip install playwright

# Installiere Playwright-Browser manuell
echo "🧩 Installiere Chromium..."
python -m playwright install chromium || exit 1
/opt/render/project/src/.venv/bin/playwright install chromium
