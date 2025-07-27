#!/usr/bin/env bash

# Stelle sicher, dass pip und playwright korrekt installiert sind
echo "📦 Installiere Python-Pakete..."
pip install --upgrade pip
pip install playwright

# Installiere Playwright-Browser manuell
echo "🧩 Installiere Chromium..."
python -m playwright install chromium || exit 1
