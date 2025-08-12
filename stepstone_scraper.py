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
OUT_DIR = "/data" if os.path.isdir("/data") else os.getcwd()
os.makedirs(OUT_DIR, exist_ok=True)

PROXY_SERVER = os.getenv("PROXY_SERVER", "http://de.decodo.com:20001")
PROXY_USER = os.getenv("PROXY_USER", "sp2ji26uar")
PROXY_PASS = os.getenv("PROXY_PASS", "l1+i6y9qSUFduqv3Sv")
LEAD_LIMIT = int(os.getenv("LEAD_LIMIT", "1000"))  # <- Neues Limit

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

def ensure_raw_header():
    """Erstellt RAW_CSV mit Header, falls nicht vorhanden (für Live-Append)."""
    if not os.path.exists(RAW_CSV):
        with open(RAW_CSV, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['keyword','location','title','company','jobs','profile'])
            writer.writeheader()

def append_raw_row(row: Dict):
    """Schreibt sofort einen Lead ins RAW-CSV (Live-Status auf Render)."""
    with open(RAW_CSV, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['keyword','location','title','company','jobs','profile'])
        writer.writerow(row)

# =============================
# Scraper
# =============================
async def scrape():
    raw_leads: List[Dict] = []
    seen_companies: Set[str] = set()
    total_hits = 0  # globaler Leadzähler
    reached_limit = False  # Flag für hartes Abbrechen bei LEAD_LIMIT

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
                hits_this_search = 0

                for page_num in range(1, PAGE_LIMIT + 1):
                    if reached_limit:
                        break
                    url = build_search_url(keyword, location, radius, page_num)
                    logger.info(f"🔍 Seite {page_num}: {url}")
                    try:
                        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                        await accept_all_cookies(page)
                        await asyncio.sleep(random.uniform(0.8, 2.0))  # Throttle
                    except Exception as e:
                        logger.warning(f"⚠️ Fehler bei {url}: {e}")
                        continue

                    if await is_access_denied(page):
                        access_denied_count += 1
                        logger.warning(f"🚫 Access denied (#{access_denied_count})")
                        if access_denied_count >= MAX_DENIED:
                            with open(PROGRESS_FILE, 'w') as f:
                                f.write(str(idx))
                            logger.warning("💤 Zu viele Access Denied – 5 Min Pause, dann Neustart")
                            await context.close(); await browser.close()
                            await asyncio.sleep(300)
                            break  # while-Loop erstellt Browser neu, idx bleibt
                        await asyncio.sleep(random.uniform(4, 8))
                        continue

                    cards = page.locator("article[data-at='job-item']")
                    count = await cards.count()
                    if count == 0:
                        break

                    for i in range(count):
                        if reached_limit:
                            break
                        card = cards.nth(i)
                        try:
                            # Strict-Mode sichere Selektoren
                            title_el = card.locator("[data-testid='job-item-title']").locator("a, div").first
                            title = (await title_el.inner_text()).strip()

                            company_el = card.locator("[data-at='job-item-company-name']").locator("a, span").first
                            company = (await company_el.inner_text()).strip()
                            if company in seen_companies:
                                continue

                            link_el = card.locator("[data-at='job-item-company-name'] a").first
                            href = await link_el.get_attribute("href")
                            if not href:
                                continue
                            profile_url = href if href.startswith("http") else f"https://www.stepstone.de{href}"

                            prof_page = await context.new_page()
                            try:
                                await prof_page.goto(profile_url, wait_until="domcontentloaded", timeout=30000)
                                await accept_all_cookies(prof_page)
                                await asyncio.sleep(random.uniform(0.8, 1.8))
                                job_count = await get_job_count(prof_page)
                                if job_count == 0:
                                    job_count = await fallback_job_search(context, company)
                            except Exception as e:
                                logger.error(f"❌ Fehler beim Profil von {company}: {e}")
                                continue
                            finally:
                                await prof_page.close()

                            logger.info(f"🔎 {company}: {job_count} Jobs")

                            if MIN_JOBS <= job_count <= MAX_JOBS:
                                seen_companies.add(company)
                                lead = {
                                    "keyword": keyword,
                                    "location": location,
                                    "title": title,
                                    "company": company,
                                    "jobs": job_count,
                                    "profile": profile_url
                                }
                                raw_leads.append(lead)
                                total_hits += 1
                                hits_this_search += 1

                                # Live: sofort ins RAW schreiben + Zwischenstand loggen
                                append_raw_row(lead)
                                logger.info(f"📥 Lead gespeichert (#{total_hits} gesamt, {hits_this_search} in aktueller Suche) → {company} [{job_count}]")

                                # ==== LIMIT-Check ====
                                if total_hits >= LEAD_LIMIT:
                                    logger.info(f"🛑 Lead-Limit erreicht ({LEAD_LIMIT}). Beende Lauf sauber …")
                                    reached_limit = True
                                    # progress speichern, damit beim nächsten Start an derselben Suche weitergemacht werden kann
                                    with open(PROGRESS_FILE, 'w') as f:
                                        f.write(str(idx))
                                    break

                        except Exception as e:
                            logger.error(f"❌ Fehler bei Jobkarte {i+1}: {e}")

                else:
                    # Schleife regulär beendet (kein Neustart und kein Limit-Halt)
                    idx += 1
                    with open(PROGRESS_FILE, 'w') as f:
                        f.write(str(idx))
                    logger.info(f"✅ Suche abgeschlossen: {keyword} in {location} – neu: {hits_this_search}, total: {total_hits}")
                    await context.close(); await browser.close()
                    continue

                # Hier gab es 'break' (Access-Denied-Neustart ODER Limit-Halt)
                await context.close(); await browser.close()
                if reached_limit:
                    break  # beendet den while-Loop vollständig
                logger.info(f"⏳ Neustart der Session – aktueller Total: {total_hits}")
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
    # CSV-Ausgabe (FINAL, dedupliziert)
    # =============================
    keys = ['keyword','location','title','company','jobs','profile']
    # RAW einlesen (kann leer sein, wenn Limit nie erreicht und keine Treffer)
    buffered = []
    if os.path.exists(RAW_CSV):
        with open(RAW_CSV, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            buffered = list(reader)

    unique_dict = {}
    for r in buffered:
        key = (r['company'], r['profile'])
        if key not in unique_dict:
            unique_dict[key] = r
    unique = list(unique_dict.values())

    with open(FINAL_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(unique)

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
