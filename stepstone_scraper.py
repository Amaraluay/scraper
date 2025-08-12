#!/usr/bin/env python3
import asyncio
import csv
import logging
import os
import random
import re
import sys
from datetime import datetime
from typing import List, Dict, Set
from urllib.parse import urlencode, quote

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from playwright_stealth import stealth_async

# =============================
# Output-/Env-Setup (Render-friendly)
# =============================
OUT_DIR = "/data" if os.path.isdir("/data") else os.getcwd()
os.makedirs(OUT_DIR, exist_ok=True)

PROXY_SERVER = os.getenv("PROXY_SERVER", "http://de.decodo.com:20001")
PROXY_USER   = os.getenv("PROXY_USER",   "sp2ji26uar")
PROXY_PASS   = os.getenv("PROXY_PASS",   "l1+i6y9qSUFduqv3Sv")
LEAD_LIMIT   = int(os.getenv("LEAD_LIMIT", "1000"))
CONCURRENCY  = int(os.getenv("CONCURRENCY", "3"))  # parallele Karten-Worker

# =============================
# Logging
# =============================
LOG_FILE = os.path.join(OUT_DIR, "stepstone_scraper.log")
logger = logging.getLogger("StepstoneScraper")
logger.setLevel(logging.INFO)
fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
ch = logging.StreamHandler(); ch.setFormatter(fmt); logger.addHandler(ch)
fh = logging.FileHandler(LOG_FILE); fh.setFormatter(fmt); logger.addHandler(fh)

# =============================
# Config
# =============================
SEARCH_PARAMS = [
    (kw, city, radius)
    for kw in [
        "gesundheits-und-krankenpfleger", "pflegehilfskraft", "servicetechniker",
        "aussendienst", "produktionsmitarbeiter", "maschinen-und-anlagenfuehrer",
        "fertigungsmitarbeiter", "kundenberater", "kundenservice", "kundendienstberater"
    ]
    for city, radius in [
        ("regensburg", 50), ("würzburg", 50), ("freiburg", 50), ("ingolstadt", 50),
        ("trier", 50), ("saarbrücken", 50), ("konstanz", 30), ("nürnberg", 50), ("passau", 50),
        ("ulm", 50), ("muenchen", 50), ("frankfurt", 50), ("augsburg", 50), ("stuttgart", 50),
        ("mannheim", 50), ("karlsruhe", 50), ("baden baden", 50), ("baden", 50)
    ]
]

PAGE_LIMIT   = 20
MIN_JOBS     = 8
MAX_JOBS     = 45
MAX_DENIED   = 5

# Timeouts / Limits
PER_CARD_TIMEOUT   = 20     # max. Dauer für EINE Karte
TITLE_TIMEOUT_MS   = 5000
COMPANY_TIMEOUT_MS = 5000
NAV_TIMEOUT_SHORT  = 8000   # 8s
