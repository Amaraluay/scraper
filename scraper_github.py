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

# -----------------------------
# Logging setup
# -----------------------------
LOG_FILE = os.path.expanduser("~/stepstone_scraper.log")
logger = logging.getLogger("StepstoneScraper")
logger.setLevel(logging.INFO)
fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
ch = logging.StreamHandler(); ch.setFormatter(fmt); logger.addHandler(ch)
fh = logging.FileHandler(LOG_FILE); fh.setFormatter(fmt); logger.addHandler(fh)

# -----------------------------
# Config
# -----------------------------
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

PROXY_SERVER = "http://de.decodo.com:20001"
PROXY_USER = "sp2ji26uar"
PROXY_PASS = "l1+i6y9qSUFduqv3Sv"

PROGRESS_FILE = "progress.txt"
ts = datetime.now().strftime("%Y%m%d_%H%M%S")
RAW_CSV = f"stepstone_raw_leads_{ts}.csv"
FINAL_CSV = f"stepstone_leads_{ts}.csv"

# -----------------------------
# Helpers
# -----------------------------
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
    txt = (await page.content()).lower()
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
        await asyncio.sleep(random.uniform(0.8, 1.8))  # Throttle
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

# -----------------------------
# Scraper
# -----------------------------
async def scrape():
    raw_leads: List[Dict] = []
    seen_companies: Set[str] = set()

    start_index = 0
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r') as f:
            try:
                start_index = int(f.read().strip())
            except:
                start_index = 0

    async with async_playwright() as pw:
        # „Restart-Loop“ statt rekursivem self-call (Fix für Punkt 2)
        idx = start_index
        while idx < len(SEARCH_PARAMS):
            access_denied_count = 0
            browser, context, page = await make_browser(pw)

            try:
                keyword, location, radius = SEARCH_PARAMS[idx]
                logger.info(f"🚀 Starte Suche {idx+1}/{len(SEARCH_PARAMS)}: {keyword} in {location}")

                for page_num in range(1, PAGE_LIMIT + 1):
                    url = build_search_url(keyword, location, radius, page_num)
                    logger.info(f"🔍 Seite {page_num}: {url}")
                    try:
                        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                        await accept_all_cookies(page)
                        await asyncio.sleep(random.uniform(0.8, 2.0))  # Throttle (Fix 6)
                    except Exception as e:
                        logger.warning(f"⚠️ Fehler bei {url}: {e}")
                        continue

                    if await is_access_denied(page):
                        access_denied_count += 1
                        logger.warning(f"🚫 Access denied (#{access_denied_count})")
                        if access_denied_count >= MAX_DENIED:
                            with open(PROGRESS_FILE, 'w') as f:
                                f.write(str(idx))
                            logger.warning("💤 Zu viele Access Denied – 5 Min Pause, dann Neustart (ohne Rekursion)")
                            await context.close(); await browser.close()
                            await asyncio.sleep(300)
                            # gehe in den while-Loop zurück → Browser/Context werden neu erstellt
                            break
                        await asyncio.sleep(random.uniform(4, 8))
                        continue

                    cards = page.locator("article[data-at='job-item']")
                    count = await cards.count()
                    if count == 0:
                        break

                    for i in range(count):
                        card = cards.nth(i)
                        try:
                            title = await card.locator("[data-testid='job-item-title'] div").inner_text()
                            company = await card.locator("span[data-at='job-item-company-name'] span").inner_text()
                            if company in seen_companies:
                                continue

                            # Firmenseite robuster finden (nicht company-logo, Fix 4 ist optional – hier nur minimaler Eingriff)
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
                                raw_leads.append({
                                    "keyword": keyword,
                                    "location": location,
                                    "title": title.strip(),
                                    "company": company.strip(),
                                    "jobs": job_count,
                                    "profile": profile_url
                                })
                        except Exception as e:
                            logger.error(f"❌ Fehler bei Jobkarte {i+1}: {e}")

                else:
                    # for-else: nur wenn nicht via 'break' (Access denied Neustart) verlassen
                    idx += 1
                    with open(PROGRESS_FILE, 'w') as f:
                        f.write(str(idx))
                    await context.close(); await browser.close()
                    continue  # Nächster SEARCH_PARAM

                # Wenn wir hier landen, gab es ein 'break' (z.B. Access-Denied-Neustart)
                # -> Loop erstellt Browser neu, idx bleibt gleich
                continue

            except Exception as e:
                logger.error(f"❌ Unerwarteter Fehler: {e}")
                try:
                    await context.close(); await browser.close()
                except:
                    pass
                await asyncio.sleep(10)
                continue

        # Ende while: alle Parameter verarbeitet

    # CSV-Ausgabe
    keys = ['keyword','location','title','company','jobs','profile']
    with open(RAW_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(raw_leads)

    unique = {(r['company'], r['profile']): r for r in raw_leads}.values()
    with open(FINAL_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(unique)

    logger.info(f"🎉 Fertig: {len(list(unique))} eindeutige Leads gespeichert.")

if __name__ == '__main__':
    try:
        asyncio.run(scrape())
    except KeyboardInterrupt:
        logger.warning("🛑 Abbruch durch Benutzer")
        sys.exit(1)
