#!/usr/bin/env python3
import asyncio
import csv
import logging
import os
import random
import re
import sys
from datetime import datetime
from typing import List, Dict, Set, Optional
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from playwright_stealth import stealth_async

# =============================
# Output-/Env-Setup (Render-friendly)
# =============================
OUT_DIR = "/data" if os.path.isdir("/data") else os.getcwd()
os.makedirs(OUT_DIR, exist_ok=True)

PROXY_SERVER = os.getenv("PROXY_SERVER")
PROXY_USER   = os.getenv("PROXY_USER")
PROXY_PASS   = os.getenv("PROXY_PASS")
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
# Config (wie Overnight)
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
        ("trier", 50), ("saarbrücken", 50), ("konstanz", 30), ("nürnberg", 50),
        ("passau", 50), ("ulm", 50), ("muenchen", 50), ("frankfurt", 50),
        ("augsburg", 50), ("stuttgart", 50), ("mannheim", 50), ("karlsruhe", 50),
        ("baden baden", 50), ("baden", 50)
    ]
]

PAGE_LIMIT = 20
MIN_JOBS   = 10
MAX_JOBS   = 50
ACCESS_DENIED_LIMIT = 10

PROGRESS_FILE = os.path.join(OUT_DIR, "progress.txt")
ts = datetime.now().strftime("%Y%m%d_%H%M%S")
RAW_CSV   = os.path.join(OUT_DIR, f"stepstone_raw_leads_{ts}.csv")
FINAL_CSV = os.path.join(OUT_DIR, f"stepstone_leads_{ts}.csv")

# =============================
# Caches
# =============================
COMPANY_UID_CACHE: Dict[str, str]   = {}  # company -> UID
COMPANY_COUNT_CACHE: Dict[str, int] = {}  # company -> jobs

# =============================
# Helpers
# =============================
def slug_city(text: str) -> str:
    repl = (("ä","ae"),("ö","oe"),("ü","ue"),("ß","ss"))
    s = text.strip().lower()
    for a,b in repl: s = s.replace(a,b)
    return re.sub(r"\s+", "-", s)

def build_search_url(keyword: str, city: str, radius: int, page_num: int) -> str:
    return f"https://www.stepstone.de/jobs/{keyword}/in-{slug_city(city)}?radius={radius}&page={page_num}&searchOrigin=Resultlist_top-search"

async def accept_all_cookies(page):
    try:
        await page.click('#ccmgt_explicit_accept', timeout=4000)
    except:
        pass

async def is_access_denied(page) -> bool:
    try:
        txt = (await page.content()).lower()
        return "access denied" in txt or "permission to access" in txt
    except:
        return False

async def get_job_count(page) -> int:
    try:
        el = await page.wait_for_selector('span.at-facet-header-total-results', timeout=8000)
        text = await el.inner_text()
        return int(re.sub(r"\D", "", text))
    except:
        return 0

def ensure_raw_header():
    if not os.path.exists(RAW_CSV):
        with open(RAW_CSV, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['keyword','location','title','company','jobs','profile'])
            writer.writeheader()

def append_raw_row(row: Dict):
    with open(RAW_CSV, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['keyword','location','title','company','jobs','profile'])
        writer.writerow(row)

def extract_company_uid_from_html(html: str) -> Optional[str]:
    pats = [
        r'companyUid=([0-9a-fA-F\-]{16,})',
        r'"companyUid"\s*:\s*"([0-9a-fA-F\-]{16,})"',
        r"data-company-uid=['\"]([0-9a-fA-F\-]{16,})['\"]",
    ]
    for pat in pats:
        m = re.search(pat, html, flags=re.IGNORECASE)
        if m:
            return m.group(1)
    return None

async def count_on_listing(browser, context, listing_url: str) -> int:
    """
    Zählt auf /jobs/?companyUid=... – die einzige Seite, die wir aktiv nutzen.
    Kurze Timeouts, Ressourcen-Blocking beschleunigt das Laden.
    """
    async def _count(ctx):
        p = await ctx.new_page()
        try:
            await stealth_async(p)
            await p.goto(listing_url, wait_until="domcontentloaded", timeout=12000)
            await accept_all_cookies(p)
            return await get_job_count(p)
        except Exception as e:
            logger.debug(f"count_on_listing fail {listing_url}: {e}")
            return 0
        finally:
            await p.close()

    # 1) aktueller Context
    c = await _count(context)
    if c > 0:
        return c

    # 2) Retry ohne Proxy (nur wenn Proxy gesetzt war)
    if PROXY_SERVER:
        try:
            logger.info("↪️ Retry Listing ohne Proxy-Context")
            no_proxy_ctx = await browser.new_context(ignore_https_errors=True)
            try:
                return await _count(no_proxy_ctx)
            finally:
                await no_proxy_ctx.close()
        except Exception as e:
            logger.debug(f"no-proxy retry fail {listing_url}: {e}")

    return 0

async def make_browser(pw):
    # Browser
    browser = await pw.chromium.launch(
        headless=True,
        args=["--no-sandbox", "--disable-http2"]
    )

    # Context
    ctx_kwargs = {
        "ignore_https_errors": True,
        "user_agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36)"),
        "locale": "de-DE",
    }
    if PROXY_SERVER:
        ctx_kwargs["proxy"] = {
            "server": PROXY_SERVER,
            **({"username": PROXY_USER} if PROXY_USER else {}),
            **({"password": PROXY_PASS} if PROXY_PASS else {}),
        }

    context = await browser.new_context(**ctx_kwargs)

    # Ressourcen-Blocking
    async def route_handler(route, request):
        if request.resource_type in ("image", "media", "font", "stylesheet"):
            return await route.abort()
        url = request.url
        if any(bad in url for bad in ("googletagmanager", "google-analytics", "doubleclick", "facebook", "hotjar", "segment.io")):
            return await route.abort()
        return await route.continue_()
    await context.route("**/*", route_handler)

    page = await context.new_page()
    await stealth_async(page)
    context.set_default_timeout(10000)
    context.set_default_navigation_timeout(15000)
    return browser, context, page

# =============================
# Main scraping
# =============================
async def scrape():
    raw_leads: List[Dict] = []
    seen_companies: Set[str] = set()
    access_denied_count = 0
    total_hits = 0

    ensure_raw_header()

    start_index = 0
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, 'r') as f:
                start_index = int(f.read().strip())
        except:
            start_index = 0

    async with async_playwright() as pw:
        browser, context, page = await make_browser(pw)

        for idx, (keyword, location, radius) in enumerate(SEARCH_PARAMS[start_index:], start=start_index):
            logger.info(f"🚀 Starte Suche {idx+1}/{len(SEARCH_PARAMS)}: {keyword} in {location}")

            for page_index in range(1, PAGE_LIMIT + 1):
                if total_hits >= LEAD_LIMIT:
                    logger.info(f"🛑 Lead-Limit erreicht ({LEAD_LIMIT}). Stoppe.")
                    with open(PROGRESS_FILE, 'w') as f: f.write(str(idx))
                    await browser.close()
                    return

                url = build_search_url(keyword, location, radius, page_index)
                logger.info(f"🔍 Seite {page_index}: {url}")
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                    await accept_all_cookies(page)
                except Exception as e:
                    logger.warning(f"⚠️ Fehler bei {url}: {e}")
                    break

                if await is_access_denied(page):
                    access_denied_count += 1
                    logger.warning(f"⚠️ Access denied auf {url} (insg. {access_denied_count})")
                    if access_denied_count >= ACCESS_DENIED_LIMIT:
                        logger.error("❌ Zu viele Access Denied – abbrechen")
                        with open(PROGRESS_FILE, 'w') as f: f.write(str(idx))
                        await browser.close()
                        return
                    await asyncio.sleep(random.randint(5, 10))
                    continue

                cards = page.locator("article[data-at='job-item']")
                count = await cards.count()
                logger.info(f"📦 Gefundene Jobkarten: {count}")
                if count == 0:
                    break

                for i in range(count):
                    if total_hits >= LEAD_LIMIT:
                        logger.info(f"🛑 Lead-Limit erreicht ({LEAD_LIMIT}). Stoppe.")
                        with open(PROGRESS_FILE, 'w') as f: f.write(str(idx))
                        await browser.close()
                        return

                    try:
                        card = cards.nth(i)

                        # Titel & Firma – wie Overnight (Achtung: Klassen können variieren)
                        title = await card.locator("[data-testid='job-item-title'] div.res-ewgtgq").inner_text()
                        company = await card.locator("span[data-at='job-item-company-name'] span.res-du9bhi").inner_text()
                        title = title.strip(); company = company.strip()
                        logger.info(f"📝 Gefunden: {title} bei {company}")

                        # Cache: sofort bedienen
                        if company in COMPANY_COUNT_CACHE:
                            cached = COMPANY_COUNT_CACHE[company]
                            logger.info(f"🔁 Cache-Treffer {company}: {cached} Jobs")
                            job_count = cached
                        else:
                            # 1) UID evtl. direkt im Namenslink
                            uid = COMPANY_UID_CACHE.get(company)
                            if not uid:
                                uid_link = card.locator("[data-at='job-item-company-name'] a[href*='companyUid=']").first
                                if await uid_link.count():
                                    href = await uid_link.get_attribute("href")
                                    m = re.search(r'companyUid=([0-9a-fA-F\-]{16,})', href or "")
                                    if m:
                                        uid = m.group(1)
                                        COMPANY_UID_CACHE[company] = uid

                            # 2) wenn noch keine UID → kurze Detailseite öffnen & UID parsen
                            if not uid:
                                job_a = card.locator("[data-testid='job-item-title'] a").first
                                job_href = await job_a.get_attribute("href") if await job_a.count() else None
                                if job_href:
                                    job_url = job_href if job_href.startswith("http") else f"https://www.stepstone.de{job_href}"
                                    d = await context.new_page(); await stealth_async(d)
                                    try:
                                        await d.goto(job_url, wait_until="commit", timeout=10000)
                                        await accept_all_cookies(d)
                                        html = await d.content()
                                        uid = extract_company_uid_from_html(html)
                                        if uid:
                                            COMPANY_UID_CACHE[company] = uid
                                    except Exception:
                                        pass
                                    finally:
                                        await d.close()

                            # 3) Zählen nur über UID-Listing (CMP wird NICHT benutzt)
                            job_count = 0
                            if uid:
                                listing_url = f"https://www.stepstone.de/jobs/?companyUid={uid}"
                                job_count = await count_on_listing(browser, context, listing_url)
                            else:
                                logger.debug(f"⚠️ Keine companyUid für {company} gefunden.")

                            # Positives Ergebnis cache’n
                            if job_count > 0:
                                COMPANY_COUNT_CACHE[company] = job_count

                        logger.info(f"🔎 {company}: {job_count} Jobs")

                        if MIN_JOBS <= job_count <= MAX_JOBS:
                            if company not in seen_companies:
                                seen_companies.add(company)
                                entry = {
                                    'keyword': keyword,
                                    'location': location,
                                    'title': title,
                                    'company': company,
                                    'jobs': job_count,
                                    'profile': f"https://www.stepstone.de/jobs/?companyUid={COMPANY_UID_CACHE.get(company,'')}" if company in COMPANY_UID_CACHE else ""
                                }
                                raw_leads.append(entry)
                                append_raw_row(entry)
                                total_hits += 1
                                logger.info(f"🚀 Lead {total_hits} gespeichert: {company} ({job_count})")

                    except Exception as e:
                        logger.error(f"❌ Fehler beim Auslesen Karte {i+1}: {e}")
                        continue

            with open(PROGRESS_FILE, 'w') as f:
                f.write(str(idx+1))

        await browser.close()

    # Dedupe & Save
    unique = { (r['company'], r['profile']): r for r in raw_leads }
    leads = list(unique.values())
    keys = ['keyword','location','title','company','jobs','profile']

    with open(RAW_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(raw_leads)
    logger.info(f"💾 Rohdaten → {RAW_CSV}")

    with open(FINAL_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(leads)
    logger.info(f"🎉 Fertig: {len(leads)} eindeutige Leads → {FINAL_CSV}")

# =============================
# Main
# =============================
if __name__ == '__main__':
    try:
        asyncio.run(scrape())
    except KeyboardInterrupt:
        logger.warning("⚠️ Abbruch durch Benutzer")
        sys.exit(1)
