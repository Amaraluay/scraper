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

# harte Obergrenzen/Timeouts gegen „Hänger“
PER_CARD_TIMEOUT = 30  # Sekunden: max. Dauer für EINE Karte
TITLE_TIMEOUT    = 7000
COMPANY_TIMEOUT  = 7000

PROGRESS_FILE = os.path.join(OUT_DIR, "progress.txt")
ts = datetime.now().strftime("%Y%m%d_%H%M%S")
RAW_CSV   = os.path.join(OUT_DIR, f"stepstone_raw_leads_{ts}.csv")
FINAL_CSV = os.path.join(OUT_DIR, f"stepstone_leads_{ts}.csv")

# =============================
# Helpers
# =============================
def slug_city(city: str) -> str:
    """Konvertiert Stadt in StepStone-kompatiblen Pfad-Slug (Umlaute, ß, Leerzeichen)."""
    repl = (("ä","ae"),("ö","oe"),("ü","ue"),("ß","ss"))
    c = city.strip().lower()
    for a,b in repl: c = c.replace(a,b)
    return c.replace(" ", "-")

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
    # kein Fehler nötig
    logger.debug("⚠️ Kein Cookie-Banner oder bereits akzeptiert")

async def is_access_denied(page) -> bool:
    try:
        txt = (await page.content()).lower()
        return ("access denied" in txt) or ("permission to access" in txt)
    except:
        return False

async def get_job_count(page) -> int:
    try:
        sel = "span.at-facet-header-total-results, [data-at='facet-total-results']"
        el = await page.wait_for_selector(sel, timeout=12000)
        text = await el.inner_text()
        return int(re.sub(r"\D", "", text))
    except:
        return 0

async def fallback_job_search(context, company) -> int:
    query = company.replace(" ", "%20")
    url = f"https://www.stepstone.de/jobs/in-deutschland?keywords={query}"
    p = await context.new_page()
    try:
        await stealth_async(p)
        await p.goto(url, wait_until="domcontentloaded", timeout=30000)
        await accept_all_cookies(p)
        await asyncio.sleep(random.uniform(0.8, 1.8))
        c = await get_job_count(p)
        logger.info(f"🔁 Fallback-Suche für {company}: {c} Jobs")
        return c
    except Exception as e:
        logger.error(f"❌ Fallback-Fehler für {company}: {e}")
        return 0
    finally:
        await p.close()

async def make_browser(pw):
    browser = await pw.chromium.launch(headless=True, args=["--no-sandbox"])
    context = await browser.new_context(
        proxy={"server": PROXY_SERVER, "username": PROXY_USER, "password": PROXY_PASS},
        user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36)")
    )
    # sinnvolle Defaults
    context.set_default_timeout(15000)
    context.set_default_navigation_timeout(30000)

    page = await context.new_page()
    await stealth_async(page)
    return browser, context, page

def ensure_raw_header():
    if not os.path.exists(RAW_CSV):
        with open(RAW_CSV, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['keyword','location','title','company','jobs','profile'])
            writer.writeheader()

def append_raw_row(row: Dict):
    with open(RAW_CSV, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['keyword','location','title','company','jobs','profile'])
        writer.writerow(row)

# =============================
# Scraper
# =============================
async def scrape():
    raw_leads: List[Dict] = []
    seen_companies: Set[str] = set()
    total_hits = 0
    reached_limit = False

    ensure_raw_header()

    start_index = 0
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, 'r') as f:
                start_index = int(f.read().strip())
        except:
            start_index = 0

    async with async_playwright() as pw:
        idx = start_index
        while idx < len(SEARCH_PARAMS) and not reached_limit:
            access_denied_count = 0
            browser, context, page = await make_browser(pw)

            try:
                keyword, location, radius = SEARCH_PARAMS[idx]
                logger.info(f"🚀 Starte Suche {idx+1}/{len(SEARCH_PARAMS)}: {keyword} in {location}")

                for page_num in range(1, PAGE_LIMIT + 1):
                    if reached_limit: break
                    url = build_search_url(keyword, location, radius, page_num)
                    logger.info(f"🔍 Seite {page_num}: {url}")
                    try:
                        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                        await accept_all_cookies(page)
                        # warten bis Karten da sind (oder leere Seite)
                        try:
                            await page.wait_for_selector("article[data-at='job-item']", timeout=15000)
                        except:
                            logger.info("ℹ️ Keine Jobkarten gefunden (evtl. leer/Layoutwechsel).")
                            break
                        await asyncio.sleep(random.uniform(0.8, 2.0))  # Throttle
                    except Exception as e:
                        logger.warning(f"⚠️ Fehler bei {url}: {e}")
                        continue

                    if await is_access_denied(page):
                        access_denied_count += 1
                        logger.warning(f"🚫 Access denied (#{access_denied_count})")
                        if access_denied_count >= MAX_DENIED:
                            with open(PROGRESS_FILE, 'w') as f: f.write(str(idx))
                            logger.warning("💤 Zu viele Access Denied – 5 Min Pause, dann Neustart")
                            await context.close(); await browser.close()
                            await asyncio.sleep(300)
                            break
                        await asyncio.sleep(random.uniform(4, 8))
                        continue

                    cards = page.locator("article[data-at='job-item']")
                    count = await cards.count()
                    logger.info(f"📦 Gefundene Jobkarten: {count}")
                    if count == 0:
                        break

                    for i in range(count):
                        if reached_limit: break
                        card = cards.nth(i)

                        async def process_card():
                            nonlocal total_hits
                            # Titel & Firma strict-safe mit individuellen Timeouts
                            title_el   = card.locator("[data-testid='job-item-title']").locator("a, div").first
                            title      = (await title_el.inner_text(timeout=TITLE_TIMEOUT)).strip()

                            company_el = card.locator("[data-at='job-item-company-name']").locator("a, span").first
                            company    = (await company_el.inner_text(timeout=COMPANY_TIMEOUT)).strip()

                            if company in seen_companies:
                                logger.debug(f"↩️ Duplikat übersprungen: {company}")
                                return

                            # ===== Firmenprofil ermitteln (Name-Link → Logo → Detailseite) =====
                            profile_url = None

                            # 1) Firmenname verlinkt
                            name_link = card.locator("[data-at='job-item-company-name'] a").first
                            if await name_link.count():
                                href = await name_link.get_attribute("href")
                                if href:
                                    profile_url = href if href.startswith("http") else f"https://www.stepstone.de{href}"

                            # 2) Firmenlogo verlinkt
                            if not profile_url:
                                logo_link = card.locator("a[data-at='company-logo']").first
                                if await logo_link.count():
                                    href = await logo_link.get_attribute("href")
                                    if href:
                                        profile_url = href if href.startswith("http") else f"https://www.stepstone.de{href}"

                            job_count = 0

                            # 3) Jobdetailseite öffnen und dort Firmenlink suchen
                            if not profile_url:
                                job_link = card.locator("[data-testid='job-item-title'] a").first
                                job_href = await job_link.get_attribute("href") if await job_link.count() else None
                                if not job_href:
                                    any_link = card.locator("a").first
                                    job_href = await any_link.get_attribute("href") if await any_link.count() else None

                                if job_href:
                                    job_url = job_href if job_href.startswith("http") else f"https://www.stepstone.de{job_href}"
                                    d = await context.new_page()
                                    try:
                                        await stealth_async(d)
                                        await d.goto(job_url, wait_until="domcontentloaded", timeout=30000)
                                        await accept_all_cookies(d)
                                        await asyncio.sleep(random.uniform(0.6, 1.2))
                                        # Firma ggf. aktualisieren
                                        try:
                                            company = (await d.locator(
                                                "[data-at='company-name'], [data-testid='company-name'], h3:has(a)"
                                            ).first.inner_text(timeout=3000)).strip()
                                        except:
                                            pass
                                        # Firmenprofil-Link in Detailseite
                                        try:
                                            comp_link = d.locator("a[href*='/cmp/'], a:has-text('Alle Jobs von'), a:has-text('Weitere Jobs')")
                                            if await comp_link.count():
                                                href = await comp_link.first.get_attribute("href")
                                                if href:
                                                    profile_url = href if href.startswith("http") else f"https://www.stepstone.de{href}"
                                        except:
                                            pass
                                    finally:
                                        await d.close()

                            # Firmenprofil zählen oder Fallback
                            if profile_url:
                                p = await context.new_page()
                                try:
                                    await stealth_async(p)
                                    await p.goto(profile_url, wait_until="domcontentloaded", timeout=30000)
                                    await accept_all_cookies(p)
                                    await asyncio.sleep(random.uniform(0.8, 1.8))
                                    job_count = await get_job_count(p)
                                    if job_count == 0:
                                        job_count = await fallback_job_search(context, company)
                                finally:
                                    await p.close()
                            else:
                                job_count = await fallback_job_search(context, company)

                            logger.info(f"🔎 {company}: {job_count} Jobs")

                            if MIN_JOBS <= job_count <= MAX_JOBS:
                                seen_companies.add(company)
                                lead = {
                                    "keyword": keyword,
                                    "location": location,
                                    "title": title,
                                    "company": company,
                                    "jobs": job_count,
                                    "profile": profile_url or ""
                                }
                                raw_leads.append(lead)
                                append_raw_row(lead)  # Live-Append
                                total_hits += 1
                                logger.info(f"📥 Lead gespeichert (#{total_hits}) → {company} [{job_count}]")

                        # Watchdog pro Karte
                        try:
                            await asyncio.wait_for(process_card(), timeout=PER_CARD_TIMEOUT)
                        except asyncio.TimeoutError:
                            logger.warning(f"⏱️ Karte {i+1}/{count} > {PER_CARD_TIMEOUT}s → skip")
                        except Exception as e:
                            logger.error(f"❌ Fehler bei Jobkarte {i+1}: {e}")

                        # kleiner Delay gegen Anti-Bot/Render-Spikes
                        await asyncio.sleep(random.uniform(0.3, 0.9))

                        # Limit prüfen
                        if total_hits >= LEAD_LIMIT:
                            logger.info(f"🛑 Lead-Limit erreicht ({LEAD_LIMIT}). Beende Lauf sauber …")
                            reached_limit = True
                            with open(PROGRESS_FILE, 'w') as f: f.write(str(idx))
                            break

                else:
                    # regulär fertig (kein Break)
                    idx += 1
                    with open(PROGRESS_FILE, 'w') as f: f.write(str(idx))
                    await context.close(); await browser.close()
                    continue

                # Break (Access-Denied-Neustart ODER Limit)
                await context.close(); await browser.close()
                if reached_limit: break
                logger.info("⏳ Neustart der Session …")
                continue

            except Exception as e:
                logger.error(f"❌ Unerwarteter Fehler: {e}")
                try:
                    await context.close(); await browser.close()
                except:
                    pass
                await asyncio.sleep(10)
                continue

    # =============================
    # FINAL CSV (dedupliziert)
    # =============================
    keys = ['keyword','location','title','company','jobs','profile']
    buffered = []
    if os.path.exists(RAW_CSV):
        with open(RAW_CSV, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f); buffered = list(reader)

    unique = {}
    for r in buffered:
        key = (r['company'], r['profile'])
        if key not in unique: unique[key] = r

    with open(FINAL_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader(); writer.writerows(unique.values())

    logger.info(f"🎉 Fertig: {len(unique)} eindeutige Leads gespeichert (von {len(buffered)} RAW-Zeilen).")
    logger.info(f"📝 RAW:   {RAW_CSV}")
    logger.info(f"📝 FINAL: {FINAL_CSV}")
    logger.info(f"🧭 LOG:   {LOG_FILE}")

# =============================
# Main
# =============================
if __name__ == '__main__':
    try:
        asyncio.run(scrape())
    except KeyboardInterrupt:
        logger.warning("🛑 Abbruch durch Benutzer")
        sys.exit(1)
