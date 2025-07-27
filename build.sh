#!/bin/bash
set -e

# Installiere Chromium mit Playwright
npx playwright install --with-deps

# Optional: Setze PATH für Render
export PATH=$PATH:/usr/bin
