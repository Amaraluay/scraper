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

from playwright.async_api import async_playwright

# =============================
# Output-/Env-Setup
# =============================
# Schreibe standardmäßig nach /data (Render-Disk), falls vorhanden
OUT_DIR = "/data" if os.path.isdir("/data") else os.getcwd()
os.makedirs(OUT_DIR, exist_ok=True)

# Proxy aus Env (sauber für Render). Fällt auf im Code gesetzte Defaults zurück.
PROXY_SERVER = os.getenv("PROXY_SERVER", "http://de.decodo.com:20001")
PROXY_USER = os.getenv("PROXY_USER", "sp2ji26uar")
PROXY_PASS = os.getenv("PROXY_PASS", "l1+i6y9qSUFduqv3Sv")

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

PAGE_LIMIT = 20
MIN_JOBS = 8
MAX_JOBS = 45
MAX_DENIED = 5

PROGRESS_FILE = os.path.join(OUT_DIR, "progress.txt")
ts = datetime.now().strftime("%Y%m%d_%H%M%S")
RAW_CSV = os.path.join(OUT_DIR, f"stepstone_raw_leads_{ts}.csv")
FINAL_CSV = os.path.join(OUT_DIR, f"stepstone_leads_{ts}.csv")

# =============================
# Helpers
# =============================
def slug_city(city: str) -> str:
    """Konvertiert Stadt in StepStone-kompatiblen Pfad-Slug (Umlaute, ß, Leerzeichen)."""
    repl = (("ä","ae"),("ö","oe"),("ü","ue"),("ß","ss"))
    c = city.strip().lower()
    for a,b in repl:
        c = c.replace(a,b)
    c = c.replace(" ", "-")
    return c

def build_search_url(keyword: str, city: str, radius: int, page_num: int) -> str:
    base = f"https://www.stepstone.de/jobs/{quote(keyword)}/in-{slug_city(city)}"
    qs = urlencode({"radius": radius, "page": page_num, "searchOrigin": "Resultlist_top-search"})
    return f"{base}?{qs}"

async def accept_all_cookies(page):
    selectors = [
        "#ccmgt_explicit_accept",
        "button:has-text('Alle akzeptieren')",
        "button:has-text('Alles akzeptieren')",
        "button[aria-label='Alle akzeptieren']",
        "button[aria-label='Alles akzeptieren']",
        "button:has-text('Accept all')",
    ]
    for s in selectors:
        try:
            btn = await page.wait_for_selector(s, timeout=2000)
            await btn.click()
            logger.debug("✅ Cookies akzeptiert")
            return
        except:
            pass
    logger.debug("⚠️ Kein Cookie-Banner oder bereits akzeptiert")

async def is_access_denied(page) -> bool:
    try:
        txt = (await page.content()).lower()
    except:
        return False
    return ("access denied" in txt) or ("permission to access" in txt)

async def get_job_count(page) -> int:
    try:
        sel = "span.at-facet-header-total-results, [data-at='facet-total-results']"
        el = await page.wait_for_selector(sel, timeout=12000)
        text = await el.inner_text()
        return int(re.sub(r"\D", "", text))
    except:
        return 0

async def fallback_job_search(context, company) -> int:
    # Keyword-Suche als Rückfall
    query = company.replace(" ", "%20")
    fallback_url = f"https://www.stepstone.de/jobs/in-deutschland?keywords={query}"
    fallback_page = await context.new_page()
    try:
        await fallback_page.goto(fallback_url, wait_until="domcontentloaded", timeout=30000)
        await accept_all_cookies(fallback_page)
        await asyncio.sleep(random.uniform(0.8, 1.8))
        count = await get_job_count(fallback_page)
        logger.info(f"🔁 Fallback-Suche für {company}: {count} Jobs")
        return count
    except Exception as e:
        logger.error(f"❌ Fallback-Fehler für {company}: {e}")
        return 0
    finally:
        await fallback_page.close()

async def make_browser(pw):
    browser = await pw.chromium.launch(headless=True, args=["--no-sandbox"])
    context = await browser.new_context(
        proxy={"server": PROXY_SERVER, "username": PROXY_USER, "password": PROXY_PASS},
        user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36)")
    )
    page = await context.new_page()
    return browser, context, page

# =============================
# Scraper
# =============================
async def scrape():
    raw_leads: List[Dict] = []
    seen_companies: Set[str] = set()

    start_index = 0
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, 'r') as f:
                start_index = int(f.read().strip())
        except:
            start_index = 0

    async with async_playwright() as pw:
        idx = start_index
        while idx < len(SEARCH_PARAMS):
            access_denied_count = 0
            brow
