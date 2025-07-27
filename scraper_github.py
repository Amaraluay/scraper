# stepstone_scraper_resume.py
import asyncio
import csv
import json
import logging
import os
import re
from datetime import datetime
from typing import List, Dict, Set
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from playwright_stealth import stealth_async

# ----------------------------------
# Konfigurationen
# ----------------------------------
SEARCH_PARAMS = [
    ("pflege", "regensburg", 50),
    ("pflege", "freiburg", 50),
    ("aussendienst", "trier", 50),
    ("maschinen-und-anlagenfuehrer", "passau", 50),
    ("kundenberater", "saarbruecken", 50),
    # Weitere Keywords & Orte nach Wunsch erweitern
]

PAGE_LIMIT = 20
MIN_JOBS = 8
MAX_JOBS = 45
MAX_DENIED = 5
STATE_FILE = "scraper_state.json"

# Proxy (optional)
PROXY = {
    "server": "http://de.decodo.com:20001",
    "username": "sp2ji26uar",
    "password": "l1+i6y9qSUFduqv3Sv"
}

# Output-Dateien
ts = datetime.now().strftime("%Y%m%d_%H%M%S")
CSV_FILE = os.path.expanduser(f"~/Desktop/stepstone_final_{ts}.csv")
LOG_FILE = os.path.expanduser(f"~/Desktop/stepstone_log.log")

# ----------------------------------
# Logging Setup
# ----------------------------------
logger = logging.getLogger("Stepstone")
logger.setLevel(logging.INFO)
fh = logging.FileHandler(LOG_FILE)
fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(fh)
logger.addHandler(logging.StreamHandler())

# ----------------------------------
# Helper-Funktionen
# ----------------------------------
async def accept_all_cookies(page):
    try:
        await page.click('#ccmgt_explicit_accept', timeout=5000)
    except:
        pass

async def is_access_denied(page):
    html = await page.content()
    return "access denied" in html.lower()

async def get_job_count(page) -> int:
    try:
        el = await page.wait_for_selector("span.at-facet-header-total-results", timeout=7000)
        txt = await el.inner_text()
        return int(re.sub(r"\D", "", txt))
    except:
        return 0

async def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return {}

async def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)

# ----------------------------------
# Main Scraper
# ----------------------------------
async def scrape():
    state = await load_state()
    raw_leads = []
    seen = set()
    denied_count = 0

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(proxy=PROXY)

        page = await context.new_page()
        await stealth_async(page)

        for keyword, location, radius in SEARCH_PARAMS:
            key_id = f"{keyword}_{location}_{radius}"
            start_page = state.get(key_id, 1)

            for page_num in range(start_page, PAGE_LIMIT + 1):
                url = f"https://www.stepstone.de/jobs/{keyword}/in-{location}?radius={radius}&page={page_num}&searchOrigin=Resultlist_top-search"
                logger.info(f"🔍 Seite {page_num}: {url}")
                try:
                    await page.goto(url, timeout=30000)
                    await accept_all_cookies(page)
                except Exception as e:
                    logger.warning(f"⚠️ Fehler: {e}")
                    continue

                if await is_access_denied(page):
                    denied_count += 1
                    if denied_count >= MAX_DENIED:
                        logger.warning("⛔ Access denied mehrfach – Wartezeit aktiv...")
                        await asyncio.sleep(300)
                        denied_count = 0
                        continue

                cards = page.locator("article[data-at='job-item']")
                count = await cards.count()
                if count == 0:
                    break

                for i in range(count):
                    card = cards.nth(i)
                    try:
                        title = await card.locator("[data-testid='job-item-title']").inner_text()
                        company = await card.locator("span[data-at='job-item-company-name']").inner_text()
                        href = await card.locator("a[data-at='company-logo']").get_attribute("href")
                        if not href:
                            continue
                        profile = href if href.startswith("http") else f"https://www.stepstone.de{href}"

                        if company in seen:
                            continue

                        prof = await context.new_page()
                        await stealth_async(prof)
                        try:
                            await prof.goto(profile, timeout=30000)
                            await accept_all_cookies(prof)
                            job_count = await get_job_count(prof)
                            if job_count == 0:
                                fallback_url = f"https://www.stepstone.de/jobs/in-deutschland?keywords={company.replace(' ', '%20')}"
                                await prof.goto(fallback_url, timeout=30000)
                                await accept_all_cookies(prof)
                                job_count = await get_job_count(prof)
                            await prof.close()
                        except:
                            await prof.close()
                            continue

                        if MIN_JOBS <= job_count <= MAX_JOBS:
                            raw_leads.append({
                                "keyword": keyword,
                                "location": location,
                                "title": title,
                                "company": company,
                                "profile": profile,
                                "jobs": job_count
                            })
                            seen.add(company)
                    except:
                        continue

                # Save progress
                state[key_id] = page_num + 1
                await save_state(state)

        await browser.close()

    # Save final leads
    keys = ["keyword", "location", "title", "company", "profile", "jobs"]
    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(raw_leads)
    logger.info(f"✅ Fertig! {len(raw_leads)} Leads gespeichert in {CSV_FILE}")

# ----------------------------------
if __name__ == "__main__":
    try:
        asyncio.run(scrape())
    except KeyboardInterrupt:
        logger.warning("⛔ Vom Benutzer abgebrochen")
